#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# tsdbd says at startup when its directory is on ext2/3/4 or XFS (macOS: APFS or HFS+) and --verify-checksums is off
# (those file systems keep no checksums of the data, so a damaged or misdirected data block would be read as wrong
# values), and says nothing with --verify-checksums or on another file system (tmpfs, ZFS, btrfs, NFS).
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

fstype() { # GNU stat; on macOS (BSD stat has no name for it) the type mount(8) gives the mount point of $1
    if [ "$(uname -s)" = Darwin ]; then
        local mp line t
        mp=$(df -P "$1" | tail -1 | awk '{print $NF}')
        mount | while IFS= read -r line; do
            case $line in *" on $mp ("*) t=${line##*" on $mp ("} && t=${t%%,*} && echo "${t%)}" && break ;; esac
        done
    else stat -f -c %T "$1"; fi
}
case $(fstype "$base") in
ext2/ext3 | ext4 | xfs | apfs | hfs) want=1 ;;
*) want=0 ;;
esac

run() { # NAME [FLAGS]: starts tsdbd on $base/NAME, waits until it serves, stops it; the log is $base/NAME.log
    # the shell that starts tsdbd below, a subshell included (not $$); bash 3.2 (macOS) has no BASHPID: the parent of
    # a process that replaced the command substitution's subshell is this shell
    local n=$1 me=${BASHPID:-$(exec sh -c 'echo $PPID')}
    shift
    "$B/tsdbd" --dir "$base/$n" --no-auth --port 0 --port-file "$base/$n.port" --exit-with-parent="$me" "$@" > "$base/$n.log" 2>&1 &
    pid=$!
    for _ in $(seq 100); do [ -s "$base/$n.port" ] && break; kill -0 "$pid" 2> /dev/null || break; sleep 0.1; done
    [ -s "$base/$n.port" ] || fail "tsdbd did not serve: $(cat "$base/$n.log")"
    [ "$(cut -d' ' -f2 "$base/$n.port")" = "$pid" ] || fail "the port file names another process"
    stop
}

run off
run on --verify-checksums
got=$(grep -c "keeps no checksums of its data" "$base/off.log")
[ "$got" -eq "$want" ] ||
    fail "$(fstype "$base"): $got warnings without --verify-checksums, want $want: $(cat "$base/off.log")"
grep -q "keeps no checksums" "$base/on.log" && fail "a warning with --verify-checksums: $(cat "$base/on.log")"
echo "test_fs_warning: ok ($(fstype "$base"), warning $([ "$want" = 1 ] && echo shown || echo not needed))"
