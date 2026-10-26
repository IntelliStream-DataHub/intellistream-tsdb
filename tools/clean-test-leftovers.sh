#!/bin/bash
# Removes the scratch directories tsdb's tests and client tests leave in /tmp and /var/tmp.
# Anything touched in the last MINUTES (default 90) is kept: another worktree's tests may be using it.
#   tools/clean-test-leftovers.sh          list what would go
#   tools/clean-test-leftovers.sh -y       remove it
#   tools/clean-test-leftovers.sh -y 30    remove what is older than 30 minutes
set -u
yes=0
[ "${1:-}" = "-y" ] && { yes=1; shift; }
min=${1:-90}
case $min in *[!0-9]*|'') echo "usage: $0 [-y] [MINUTES]" >&2; exit 2;; esac

list() {
    find /var/tmp -maxdepth 1 -user "$(id -u)" \( -name 'tsdb-py-*' -o -name 'tsdb-embedded-*' -o -name 'tsdb-export.*' \) \
        -mmin +"$min" -print
    find /tmp -maxdepth 1 -user "$(id -u)" -type d -name 'tsdb-*' -mmin +"$min" -print
}

mapfile -t dirs < <(list)
if [ ${#dirs[@]} -eq 0 ]; then
    echo "nothing older than $min minutes"
    exit 0
fi
du -sch "${dirs[@]}" 2>/dev/null | tail -1 | sed "s/total/in ${#dirs[@]} directories/"
if [ $yes -eq 0 ]; then
    printf '  %s\n' "${dirs[@]}"
    echo "run with -y to remove them"
    exit 0
fi
rm -rf -- "${dirs[@]}"
echo "removed ${#dirs[@]} directories"
