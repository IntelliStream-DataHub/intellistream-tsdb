#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd that fails at the START number $FAILSTART_AT (default 3; the first is the series' own server, the follower and
# the clean servers of the final checks count too), for the jepsen self-test of how the harness tells an environment failure from
# damage (round 11):
#   FAILSTART=env     says what a machine out of memory says ("tsdbd: io_uring: Cannot allocate memory") and exits 1:
#                     the environment, a harness error (exit 2), never a verdict about tsdb
#   FAILSTART=damage  first damages a part file of the database (bytes of its header) and then runs the real tsdbd, whose open
#                     must refuse it (corrupt data): the data a fault left is damaged, a VIOLATION (exit 1)
# The count is kept in a file beside the directory. The follower (--follow) is left alone.
for a in "$@"; do [ "$a" = --follow ] && exec "${REAL_TSDBD:-./build/tsdbd}" "$@"; done
d=; prev=
for a in "$@"; do [ "$prev" = --dir ] && d=$a; prev=$a; done
c=$d.failstart-count
n=$(cat "$c" 2>/dev/null || echo 0)
n=$((n + 1))
echo $n > "$c"
if [ "$n" = "${FAILSTART_AT:-3}" ]; then
    case ${FAILSTART:-env} in
    env)
        # the start-up note of a file system without checksums comes first, as on a real run (it says "checksums" and "damaged": no damage)
        echo "tsdbd: $d is on ext4, which keeps no checksums of its data, and --verify-checksums is off: a damaged or misdirected data block would be read as wrong values" >&2
        echo "tsdbd: io_uring: Cannot allocate memory" >&2
        exit 1 ;;
    damage)
        f=$(ls -S "$d"/p*.part 2>/dev/null | head -1)
        [ -n "$f" ] && printf 'DAMAGE-DAMAGE-DAMAGE-DAMAGE' | dd of="$f" bs=1 seek=8 conv=notrunc 2>/dev/null ;;
    esac
fi
exec "${REAL_TSDBD:-./build/tsdbd}" "$@"
