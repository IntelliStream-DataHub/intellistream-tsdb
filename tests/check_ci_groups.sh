#!/bin/bash
# The ThreadSanitizer and ASan jobs of .github/workflows/ci.yml run `make test` as its groups (Makefile: TEST_GROUPS, and
# `test` itself with TEST_GROUPS= empty), one matrix entry each. A group missing there would silently not run under the
# sanitizers, so this fails when a group of the Makefile is not a `target:` of both jobs, or a matrix `target:` is none.
set -eu
cd "$(dirname "$0")/.."
groups=$(sed -n 's/^TEST_GROUPS ?= *//p' Makefile)
[ -n "$groups" ] || { echo "check_ci_groups: no TEST_GROUPS in the Makefile"; exit 1; }
bad=0
for g in test $groups; do
    for job in asan tsan; do
        n=$(awk -v job="$job" -v g="$g" '
            /^  [a-z0-9_-]+:/ { cur = $1; sub(":", "", cur) }
            cur == job && $0 ~ ("target: " g "[ ,}]") { c++ }
            END { print c + 0 }' .github/workflows/ci.yml)
        if [ "$n" -lt 1 ]; then echo "check_ci_groups: $g is not a matrix target of the $job job"; bad=1; fi
    done
done
# the server tests (test-net) run on both backends, one matrix entry each, in both sanitizer jobs
for job in asan tsan; do
    for io in uring epoll; do
        n=$(awk -v job="$job" -v io="$io" '
            /^  [a-z0-9_-]+:/ { cur = $1; sub(":", "", cur) }
            cur == job && $0 ~ ("target: test-net, io: " io "[ ,}]") { c++ }
            END { print c + 0 }' .github/workflows/ci.yml)
        if [ "$n" -lt 1 ]; then echo "check_ci_groups: test-net on io: $io is not a matrix entry of the $job job"; bad=1; fi
    done
done
for t in $(sed -n 's/.*{ name: [^,]*, target: \([a-z0-9-]*\).*/\1/p' .github/workflows/ci.yml | sort -u); do
    case " test $groups " in *" $t "*) ;; *) echo "check_ci_groups: matrix target $t is not a group of the Makefile"; bad=1 ;; esac
done
[ $bad -eq 0 ] && echo "check_ci_groups: ok ($groups)"
exit $bad
