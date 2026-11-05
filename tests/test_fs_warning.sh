#!/bin/bash
# tsdbd says at startup when its directory is on ext2/3/4 or XFS and --verify-checksums is off (those file systems keep
# no checksums of the data, so a damaged or misdirected data block would be read as wrong values), and says nothing
# with --verify-checksums or on another file system (tmpfs, ZFS, btrfs, NFS).
#
#   B=build tests/test_fs_warning.sh      (make test runs it; FS_TEST_DIR picks the file system, default /var/tmp)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
base=$(mktemp -d "${FS_TEST_DIR:-/var/tmp}/tsdb-fswarn.XXXXXX")
pid=
stop() { [ -n "$pid" ] && [ "$pid" -gt 1 ] 2> /dev/null && kill "$pid" 2> /dev/null && wait "$pid" 2> /dev/null; pid=; }
trap 'stop; rm -rf "$base"' EXIT
fail() { echo "test_fs_warning: FAIL: $*" >&2; exit 1; }

case $(stat -f -c %T "$base") in
ext2/ext3 | ext4 | xfs) want=1 ;;
*) want=0 ;;
esac

run() { # NAME [FLAGS]: starts tsdbd on $base/NAME, waits until it serves, stops it; the log is $base/NAME.log
    local n=$1
    shift
    "$B/tsdbd" --dir "$base/$n" --no-auth --port 0 --port-file "$base/$n.port" --exit-with-parent "$@" > "$base/$n.log" 2>&1 &
    pid=$!
    for _ in $(seq 100); do [ -s "$base/$n.port" ] && break; kill -0 "$pid" 2> /dev/null || break; sleep 0.1; done
    [ -s "$base/$n.port" ] || fail "tsdbd did not serve: $(cat "$base/$n.log")"
    [ "$(cut -d' ' -f2 "$base/$n.port")" = "$pid" ] || fail "the port file names another process"
    stop
}

run off
run on --verify-checksums
got=$(grep -c "keeps no checksums of its data" "$base/off.log")
[ "$got" -eq "$want" ] || fail "$(stat -f -c %T "$base"): $got warnings without --verify-checksums, want $want: $(cat "$base/off.log")"
grep -q "keeps no checksums" "$base/on.log" && fail "a warning with --verify-checksums: $(cat "$base/on.log")"
echo "test_fs_warning: ok ($(stat -f -c %T "$base"), warning $([ "$want" = 1 ] && echo shown || echo not needed))"
