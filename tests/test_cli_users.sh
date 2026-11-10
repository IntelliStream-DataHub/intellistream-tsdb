#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# `tsdb <dir> user ...` (tools/users-cli.c): the verbs work on USERS alone, beside a database or before there is
# one; passwords come from --password-file, TSDB_PASSWORD or a terminal and never from argv; selectors and rights
# are checked and normalized; the verifier stored for a password is the one SCRAM expects (RFC 7677: the salt and
# iterations of the file and the password give its StoredKey and ServerKey, checked with Python's hashlib).
#
#   B=build tests/test_cli_users.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-cli-users.XXXXXX")
trap 'rm -rf "$dir"' EXIT
errors=0
fail() { echo "test_cli_users: FAIL: $*" >&2; errors=$((errors + 1)); }
u() { "$B/tsdb" "$dir/db" user "$@"; }
fsize() { stat -c %s "$1" 2>/dev/null || stat -f %z "$1"; } # GNU, then BSD (macOS) stat
fmode() { stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"; }
nosid() { # runs a command in a new session, without a controlling terminal (macOS has no setsid command)
    if command -v setsid >/dev/null; then
        setsid "$@"
    else
        perl -MPOSIX -e 'POSIX::setsid() >= 0 or die; exec @ARGV' "$@"
    fi
}
refused() { # refused <what> <args...>: the command must fail and leave USERS as it was
    local what=$1
    shift
    local before
    before=$(cat "$dir/db/USERS" 2>/dev/null | od -An -tx1 | tr -d ' \n')
    if u "$@" </dev/null >/dev/null 2>&1; then fail "$what was accepted"; fi
    [ "$before" = "$(cat "$dir/db/USERS" 2>/dev/null | od -An -tx1 | tr -d ' \n')" ] || fail "$what changed USERS"
}

# the first superuser, before the database exists (the quick start's order)
TSDB_PASSWORD=pencil u add root --superuser >/dev/null || fail "add root"
[ "$(fmode "$dir/db/USERS")" = 600 ] || fail "USERS is not mode 600"
printf 'hunter2\nignored\n' > "$dir/pw"
u add acme/bob --password-file "$dir/pw" >/dev/null || fail "add with --password-file"
u list | grep -q $'^root\tuid 1\tsuperuser$' || fail "list root"
u list | grep -q $'^acme/bob\tuid 2$' || fail "list bob"

# passwords never from argv, never empty, never without a source
refused "--password on the command line" add x --password secret
refused "--password=... on the command line" add x --password=secret
TSDB_PASSWORD= refused "an empty password" add x
env -u TSDB_PASSWORD nosid "$B/tsdb" "$dir/db" user add y </dev/null >/dev/null 2>&1 \
    && fail "a password with no terminal and no file was accepted"
TSDB_PASSWORD=x refused "an existing user" add acme/bob
TSDB_PASSWORD=x refused "a bad user name" add /root
TSDB_PASSWORD=x refused "a user name with a space" add "a b"
TSDB_PASSWORD=x refused "too few iterations" add x --iterations 4095

# grants: rights words, selectors, normal forms
u grant acme/bob read,insert prefix:acme/ 2>/dev/null || fail "grant prefix"
u grant acme/bob delete 'glob:acme/*/t?' || fail "grant glob"
u grant acme/bob admin 'glob:acme/x/*' 2>"$dir/err" || fail "grant glob that is a prefix"
grep -q 'stored as that' "$dir/err" || fail "no note that a glob is stored as a prefix"
u grant acme/bob read prefix:acmecorp 2>"$dir/err" || fail "grant prefix without /"
grep -q 'warning' "$dir/err" || fail "no warning for a prefix that does not end in /"
u list > "$dir/list"
grep -q $'^\tread,insert\tprefix:acme/$' "$dir/list" || fail "list: prefix grant"
grep -q $'^\tdelete\tglob:acme/\\*/t?$' "$dir/list" || fail "list: glob grant"
grep -q $'^\tread,insert,delete,admin\tprefix:acme/x/$' "$dir/list" \
    || grep -q $'^\tadmin\tprefix:acme/x/$' "$dir/list" || fail "list: normalized glob"
refused "unknown rights" grant acme/bob reads all
refused "empty rights" grant acme/bob "" all
refused "a bad selector" grant acme/bob read acme/
refused "a glob with a lone backslash" grant acme/bob read 'glob:a\'
refused "a glob over 256 bytes" grant acme/bob read "glob:$(printf 'a%.0s' $(seq 257))*?"
refused "a grant to nobody" grant nobody read all
refused "too many arguments" grant acme/bob read all extra

u revoke acme/bob read prefix:acme/ || fail "revoke"
u list | grep -q $'^\tinsert\tprefix:acme/$' || fail "revoke left insert"
u revoke acme/bob insert prefix:acme/ || fail "revoke the rest"
u list | grep -q 'prefix:acme/$' && fail "a grant with no rights left is still listed"
# REVIEW1 #4: a revoke of what is not held fails and says so; one of a right admin implies takes admin with it
refused "a revoke of a grant not held" revoke acme/bob read prefix:acme/
u revoke acme/bob read name:nothing 2>"$dir/err" && fail "a revoke of a grant not held exits 0"
grep -q 'nothing revoked' "$dir/err" || fail "no message for a revoke of a grant not held"
u grant acme/bob admin prefix:acme/ || fail "grant admin"
u revoke acme/bob delete prefix:acme/ 2>"$dir/err" || fail "revoke delete from admin"
grep -q 'admin on prefix:acme/ is revoked too; acme/bob keeps read,insert there' "$dir/err" ||
    fail "no message when admin goes with delete"
u list | grep -q $'^\tread,insert\tprefix:acme/$' || fail "delete revoked from admin did not leave read,insert"
u revoke acme/bob write prefix:acme/ 2>/dev/null || fail "revoke write"
u list | grep -q $'^\tread\tprefix:acme/$' || fail "revoke write did not leave read"
u revoke acme/bob read prefix:acme/ || fail "revoke the last read"

# alter, passwd, drop; the last superuser may go offline, with a warning
u alter acme/bob --superuser || fail "alter --superuser"
u list | grep -q $'^acme/bob\tuid 2\tsuperuser$' || fail "bob is not superuser"
u alter acme/bob --no-superuser || fail "alter --no-superuser"
TSDB_PASSWORD=new u passwd acme/bob || fail "passwd"
TSDB_PASSWORD=x refused "passwd of nobody" passwd nobody
u drop acme/bob || fail "drop"
u list | grep -q acme/bob && fail "dropped bob is listed"
TSDB_PASSWORD=x u add acme/bob >/dev/null || fail "add bob again"
u list | grep -q $'^acme/bob\tuid 3$' || fail "bob again has a new uid"
u drop root 2>"$dir/err" || fail "drop the last superuser offline"
grep -q 'no superuser is left' "$dir/err" || fail "no warning when the last superuser goes"
TSDB_PASSWORD=pencil u add root --superuser >/dev/null || fail "add root again"

# compact keeps every fact
u list > "$dir/before"
s0=$(fsize "$dir/db/USERS")
u compact || fail "compact"
u list | diff - "$dir/before" >/dev/null || fail "compact changed the users"
[ "$(fsize "$dir/db/USERS")" -lt "$s0" ] || fail "compact did not shrink USERS"
refused "an unknown verb" frobnicate
"$B/tsdb" "$dir/nowhere" user list >/dev/null 2>&1 && fail "list of a missing directory"

# the stored verifier is SCRAM's for the password (Python's hashlib computes it independently)
if command -v python3 >/dev/null; then
    python3 -I - "$dir/db/USERS" <<'EOF' || fail "the stored verifier is not SCRAM-SHA-256's of the password"
import hashlib, hmac, struct, sys
data = open(sys.argv[1], "rb").read()
pos, found = 0, {}
names = {}
while pos + 16 <= len(data):
    ln, typ = struct.unpack_from("<IB", data, pos)
    body = data[pos + 5: pos + 5 + ln]
    if typ == 2:
        uid, flags, nl = struct.unpack_from("<IBH", body)
        names[uid] = body[7:7 + nl].decode()
    if typ == 3:
        uid, it, sl = struct.unpack_from("<IIB", body)
        salt = body[9:9 + sl]
        found[names[uid]] = (it, salt, body[9 + sl:9 + sl + 32], body[9 + sl + 32:9 + sl + 64])
    pos += ((5 + ln + 7) & ~7) + 8
it, salt, stored, server = found["root"]
sp = hashlib.pbkdf2_hmac("sha256", b"pencil", salt, it)
ck = hmac.new(sp, b"Client Key", "sha256").digest()
assert it == 4096 and len(salt) == 16, (it, len(salt))
assert hashlib.sha256(ck).digest() == stored
assert hmac.new(sp, b"Server Key", "sha256").digest() == server
EOF
fi

# REVIEW3 #2: the password rule of every client: 1 to 1023 bytes; a file's first line without "\n" or "\r\n"
p1023=$(printf 'p%.0s' $(seq 1023))
printf '%s' "$p1023" > "$dir/p1023"
printf '%s\r\n' "$p1023" > "$dir/p1023crlf"
printf '%sx\n' "$p1023" > "$dir/p1024"
printf '\nsecond\n' > "$dir/pempty"
printf '1\n' > "$dir/p1"
refused "a password file whose first line is empty" add x --password-file "$dir/pempty"
refused "a password file of 1024 bytes" add x --password-file "$dir/p1024"
refused "a missing password file" add x --password-file "$dir/nonexistent"
TSDB_PASSWORD="${p1023}x" refused "TSDB_PASSWORD of 1024 bytes" add x
u add pw/one --password-file "$dir/p1" >/dev/null || fail "a password of 1 byte"
u add pw/max --password-file "$dir/p1023crlf" >/dev/null || fail "a password of 1023 bytes and CR LF"
TSDB_PASSWORD="$p1023" u passwd pw/max || fail "TSDB_PASSWORD of 1023 bytes"
u add pw/max2 --password-file "$dir/p1023" >/dev/null || fail "a password of 1023 bytes without a line end"
u drop pw/one && u drop pw/max && u drop pw/max2 || fail "drop the password users"

# REVIEW3 #7: one rights grammar (server/users.c rights_parse): spaces around names, "none", "0xHH"; what the CLI
# prints parses back. The 15 non-empty combinations of the four rights, granted again from their printed form.
names=(read insert delete admin)
TSDB_PASSWORD=x u add rt/a >/dev/null && TSDB_PASSWORD=x u add rt/b >/dev/null || fail "add rt/a, rt/b"
for r in $(seq 1 15); do
    words=()
    for i in 0 1 2 3; do (( r >> i & 1 )) && words+=("${names[$i]}"); done
    text=$(IFS=,; echo "${words[*]}")
    u grant rt/a "$text" "name:rt/$r" || fail "grant $text"
done
u list | awk -F'\t' '$3 ~ /^name:rt\// {print $2 "\t" $3}' > "$dir/printed"
[ "$(wc -l < "$dir/printed")" -eq 15 ] || fail "rights round trip: $(wc -l < "$dir/printed") grants listed, not 15"
while IFS=$'\t' read -r text sel; do u grant rt/b "$text" "$sel" || fail "printed rights $text do not parse"; done \
    < "$dir/printed"
u list | awk -F'\t' '$3 ~ /^name:rt\// {print $2 "\t" $3}' | sort | uniq -c | awk '$1 != 2' | grep -q . \
    && fail "rights printed for rt/b differ from rt/a's"
u grant rt/b 'read , insert' name:rt/sp || fail "rights with spaces around the comma"
u list | grep -q $'^\tread,insert\tname:rt/sp$' || fail "rights with spaces: not read,insert"
refused "rights none (nothing to grant)" grant rt/b none name:rt/none
refused "rights 0x10 (no such right yet)" grant rt/b 0x10 name:rt/none
refused "rights with a tab" grant rt/b $'read\t' name:rt/none
u drop rt/a && u drop rt/b || fail "drop rt/a, rt/b"

# beside a database: the engine opens with USERS in its directory, and user commands do not open the engine
"$B/tsdb" "$dir/db" create s1 >/dev/null || fail "a database in the directory of USERS"
u list | grep -q root || fail "list beside a database"

# as root on a directory another user owns, with links planted there (tests/test_cli_root.sh): in a user namespace
# where this user is root (podman unshare), or directly when this is root
mkdir "$dir/root" && cp "$B/tsdb" "$dir/root/tsdb" && chmod 755 "$dir" "$dir/root" "$dir/root/tsdb"
if ! command -v setpriv > /dev/null; then
    echo "test_cli_users: the root cases are skipped: no setpriv"
elif [ "$(id -u)" = 0 ]; then
    bash tests/test_cli_root.sh "$dir/root/tsdb" "$dir/root/s" || fail "the root cases"
elif command -v podman > /dev/null && podman unshare true 2> /dev/null; then
    podman unshare bash tests/test_cli_root.sh "$dir/root/tsdb" "$dir/root/s" || fail "the root cases"
    podman unshare rm -rf "$dir/root" # files of another uid of the namespace
else
    echo "test_cli_users: the root cases are skipped: not root, and no podman unshare"
fi

[ "$errors" = 0 ] && echo "test_cli_users: ok" || { echo "test_cli_users: $errors failures" >&2; exit 1; }
