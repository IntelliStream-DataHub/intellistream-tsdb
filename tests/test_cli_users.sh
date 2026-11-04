#!/bin/bash
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
[ "$(stat -c %a "$dir/db/USERS")" = 600 ] || fail "USERS is not mode 600"
printf 'hunter2\nignored\n' > "$dir/pw"
u add acme/bob --password-file "$dir/pw" >/dev/null || fail "add with --password-file"
u list | grep -q $'^root\tuid 1\tsuperuser$' || fail "list root"
u list | grep -q $'^acme/bob\tuid 2$' || fail "list bob"

# passwords never from argv, never empty, never without a source
refused "--password on the command line" add x --password secret
refused "--password=... on the command line" add x --password=secret
TSDB_PASSWORD= refused "an empty password" add x
env -u TSDB_PASSWORD setsid "$B/tsdb" "$dir/db" user add y </dev/null >/dev/null 2>&1 \
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
s0=$(stat -c %s "$dir/db/USERS")
u compact || fail "compact"
u list | diff - "$dir/before" >/dev/null || fail "compact changed the users"
[ "$(stat -c %s "$dir/db/USERS")" -lt "$s0" ] || fail "compact did not shrink USERS"
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

# beside a database: the engine opens with USERS in its directory, and user commands do not open the engine
"$B/tsdb" "$dir/db" create s1 >/dev/null || fail "a database in the directory of USERS"
u list | grep -q root || fail "list beside a database"

[ "$errors" = 0 ] && echo "test_cli_users: ok" || { echo "test_cli_users: $errors failures" >&2; exit 1; }
