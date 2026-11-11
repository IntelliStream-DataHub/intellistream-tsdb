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
cleanup() {
    [ -n "$srv" ] && kill "$srv" 2>/dev/null && wait "$srv" 2>/dev/null
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
# a flag on the command line wins over the file's false
cfg flag.properties 'storage.dir = /x' 'auth.enabled = false'
out=$("$B/tsdbd" --config "$dir/flag.properties" --check-config 2>&1)
check "auth off from the file" "$(echo "$out" | grep '^auth.enabled')" "auth.enabled = false"

# 6. a tsdbd started from a file serves with its settings
cfg serve.properties "storage.dir = $dir/db" 'server.listen = 127.0.0.1' 'server.port = 0' \
    "server.port.file = $dir/port" 'server.workers = 3' 'metrics.port = auto' 'server.io = epoll'
TSDB_PASSWORD=pencil "$B/tsdb" "$dir/db" user add root --superuser > /dev/null || fail "add root"
"$B/tsdbd" --config "$dir/serve.properties" --exit-with-parent=$me > "$dir/log" 2>&1 &
srv=$!
for _ in $(seq 1 100); do
    [ -s "$dir/port" ] && break
    kill -0 "$srv" 2>/dev/null || break
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
    kill "$srv"
    wait "$srv" 2>/dev/null
    srv=""
fi
cfg cli-bad.properties 'client.server = x' 'client.nope = 1'
refused "CLI unknown key" 'cli-bad.properties:2' "$B/tsdb" --config "$dir/cli-bad.properties" --check-config

if [ $errors = 0 ]; then echo "test_config: ok"; else echo "test_config: $errors failure(s)"; fi
exit $((errors > 0))
