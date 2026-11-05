#!/bin/bash
# `tsdb <dir> ...` run as root on a directory that another user owns (secrel cli #1). The owner (the service
# account, or whoever took it over) plants symbolic and hard links where USERS, USERS.compact and the engine's files
# go; root then runs the CLI, as the docs allow. No file outside the directory may be created, written, chowned or
# chmodded: the CLI becomes the directory's owner before it opens anything there, and USERS and USERS.compact are
# never opened through a link.
#
#   tests/test_cli_root.sh TSDB SCRATCH     as root: test_cli_users.sh runs it under `podman unshare` (where this
#                                           user is root and uid 4242 another user), or directly when it is root
# TSDB and SCRATCH must be reachable by uid 4242 (test_cli_users.sh copies the binary).
set -u
T=$1
S=$2
O=4242 # the directory's owner: the service account
# make asan: LeakSanitizer stops the world with ptrace, which a process that gave up root may not do (it is no longer
# dumpable, on purpose: the owner must not read what root gave it, such as TSDB_PASSWORD). ASan itself still runs.
export ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}detect_leaks=0"
errors=0
fail() { echo "test_cli_root: FAIL: $*" >&2; errors=$((errors + 1)); }
as_owner() { setpriv --reuid $O --regid $O --clear-groups "$@"; }
[ "$(id -u)" = 0 ] || { echo "test_cli_root: must run as root (podman unshare)" >&2; exit 2; }
rm -rf "$S"
mkdir -p "$S/out"
chmod 755 "$S" "$S/out"
out=$S/out # stands for /etc: root's, mode 755
printf 'root:x:0:0:root:/root:/bin/bash\n' > "$out/passwd"
chmod 644 "$out/passwd"
printf 'root:$6$secret:19000:0:99999:7:::\n' > "$out/shadow"
chmod 640 "$out/shadow"
printf 'victim\n' > "$out/hard"
chmod 644 "$out/hard"
snapshot() { (cd "$out" && for f in *; do stat -c '%n %u %g %a %s %h' "$f"; md5sum < "$f"; done); }
before=$(snapshot)
mkdb() { mkdir "$S/$1" && chown $O:$O "$S/$1"; }
add() { TSDB_PASSWORD=pw1234567 "$@" user add admin --superuser; }

# 1. USERS a dangling link (to /etc/ld.so.preload: root code execution once its owner may write it)
mkdb d1
as_owner ln -s "$out/ld.so.preload" "$S/d1/USERS"
add "$T" "$S/d1" > "$S/d1.err" 2>&1 && fail "user add through a dangling USERS link was accepted"
grep -q "running as uid $O" "$S/d1.err" || fail "user add as root did not say it runs as the owner"
[ -e "$out/ld.so.preload" ] && fail "user add created the link's target"

# 2. USERS a link to an existing file: even list chmodded it
mkdb d2
as_owner ln -s "$out/passwd" "$S/d2/USERS"
"$T" "$S/d2" user list > /dev/null 2>&1 && fail "user list through a USERS link was accepted"
add "$T" "$S/d2" > /dev/null 2>&1 && fail "user add through a USERS link was accepted"

# 3. USERS a hard link to root's file (where fs.protected_hardlinks is off; root makes it here)
mkdb d3
ln "$out/hard" "$S/d3/USERS"
"$T" "$S/d3" user list > /dev/null 2>&1 && fail "user list of a hard-linked USERS was accepted"
add "$T" "$S/d3" > /dev/null 2>&1 && fail "user add to a hard-linked USERS was accepted"
rm "$S/d3/USERS"

# 4. USERS.compact a link to an existing file (/etc/shadow), then a dangling one: compact makes its own file
mkdb d4
add as_owner "$T" "$S/d4" > /dev/null 2>&1 || fail "user add as the owner"
for target in "$out/shadow" "$out/new"; do
    as_owner ln -s "$target" "$S/d4/USERS.compact"
    "$T" "$S/d4" user compact 2> /dev/null || fail "user compact as root"
    [ -e "$out/new" ] && fail "user compact created the link's target"
    [ -L "$S/d4/USERS" ] && fail "user compact moved the link onto USERS"
    [ "$(stat -c '%F %u %a %h' "$S/d4/USERS")" = "regular file $O 600 1" ] || fail "USERS after compact as root"
done
"$T" "$S/d4" user list 2> /dev/null | grep -q '^admin' || fail "the users after compact as root"

# 5. the same as the owner, not root
as_owner ln -s "$out/shadow" "$S/d4/USERS.compact"
as_owner "$T" "$S/d4" user compact 2> /dev/null || fail "user compact as the owner"
mkdb d5
as_owner ln -s "$out/passwd" "$S/d5/USERS"
as_owner "$T" "$S/d5" user list > /dev/null 2>&1 && fail "user list through a USERS link as the owner was accepted"

# 6. every command, not only user: the engine's files belong to the owner, and links to /etc lead nowhere
mkdb d6
"$T" "$S/d6" create s1 > /dev/null 2>&1 || fail "create as root"
[ -z "$(find "$S/d6" ! -user $O)" ] || fail "create as root made files that are not the owner's: $(find "$S/d6" ! -user $O)"
mkdb d7
for f in VOLUMES MANIFEST ID META; do as_owner ln -s "$out/$f" "$S/d7/$f"; done
"$T" "$S/d7" create s1 > /dev/null 2>&1
"$T" "$S/d7" volume add "$S/vol" > /dev/null 2>&1
for f in VOLUMES MANIFEST ID META; do [ -e "$out/$f" ] && fail "the engine created $f through a link as root"; done
# a database that does not exist yet, in a directory of the owner's: made as the owner
mkdb d8
"$T" "$S/d8/new" create s1 > /dev/null 2>&1 || fail "a new database as root"
[ "$(stat -c %u "$S/d8/new" 2> /dev/null)" = $O ] || fail "a new database in the owner's directory is not the owner's"

# 7. a directory of root's: root stays root, nothing to drop to
mkdir "$S/d9"
add "$T" "$S/d9" > "$S/d9.err" 2>&1 || fail "user add in root's directory"
grep -q "running as" "$S/d9.err" && fail "root's own directory: said it runs as another user"
[ "$(stat -c '%u %a' "$S/d9/USERS")" = "0 600" ] || fail "USERS in root's directory"

[ "$before" = "$(snapshot)" ] || fail "files outside the database changed: $(diff <(echo "$before") <(snapshot))"
[ "$(ls "$out" | wc -l)" = 3 ] || fail "files outside the database were made: $(ls "$out")"
[ "$errors" = 0 ] && echo "test_cli_root: ok" || { echo "test_cli_root: $errors failures" >&2; exit 1; }
