#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The ThreadSanitizer and ASan jobs of .github/workflows/ci.yml run `make test` as its groups (Makefile: TEST_GROUPS, and
# `test` itself with TEST_GROUPS= empty), one matrix entry each. A group missing there would silently not run under the
# sanitizers, so this fails when a group of the Makefile is not a `target:` of both jobs, or a matrix `target:` is none.
set -eu
cd "$(dirname "$0")/.."
# run from `make test TEST_GROUPS=` (the sanitizer jobs): the empty override must not reach the print-var below
unset MAKEFLAGS MFLAGS MAKEOVERRIDES TEST_GROUPS
groups=$(make -s --no-print-directory print-var VAR=TEST_GROUPS)
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
for t in $(awk '/^  [a-z0-9_-]+:/ { cur = $1; sub(":", "", cur) } cur == "asan" || cur == "tsan" { print }' .github/workflows/ci.yml \
        | sed -n 's/.*{ name: [^,]*, target: \([a-z0-9-]*\).*/\1/p' | sort -u); do
    case " test $groups " in *" $t "*) ;; *) echo "check_ci_groups: matrix target $t is not a group of the Makefile"; bad=1 ;; esac
done
# test_cluster's scenarios: every one of the table all[] in exactly one of the CLUSTER_SCEN_N groups
want=$(awk '/} all\[\] = \{/ {on = 1} on {print} on && /};/ {exit}' tests/test_cluster.c | grep -o '{"[a-z0-9_]*"' | tr -d '{"' | sort)
have=$(for g in $groups; do case $g in test-cluster-[0-9]*) make -s --no-print-directory print-var VAR=CLUSTER_SCEN_${g#test-cluster-} ;; esac; done | tr ' ' '\n' | grep . | sort)
[ -n "$want" ] || { echo "check_ci_groups: no scenario table in tests/test_cluster.c"; exit 1; }
if [ "$want" != "$have" ]; then
    echo "check_ci_groups: test_cluster's scenarios and the CLUSTER_SCEN_N groups differ:"
    diff <(echo "$want") <(echo "$have") | sed 's/^/  /' || true
    bad=1
fi

CI=.github/workflows/ci.yml
# the matrix lines of one job of ci.yml
job_lines() { awk -v job="$1" '/^  [a-z0-9_-]+:/ { cur = $1; sub(":", "", cur) } cur == job' "$CI"; }
# every group target the Makefile defines is run by `make test` (TEST_GROUPS) or is test-units-b itself: one dropped from
# UNITS_GROUPS and from both matrices at once would otherwise run nowhere (tools/mutants lines: `make -n test` shows none)
defined=$(make -s --no-print-directory -qp 2>/dev/null | grep -oE '^(test-units-[a-z0-9]+|test-(meta|cluster)-[0-9]+|test-meta-mutants-[0-9]+|test-links|test-net):' | tr -d : | sort -u)
for t in $defined; do
    case " test-units-b $groups " in *" $t "*) ;; *) echo "check_ci_groups: $t is defined in the Makefile and is no TEST_GROUPS group"; bad=1 ;; esac
done
# the plain-build job: `make test TEST_GROUPS=` and the BT_GROUPS_N, which together are TEST_GROUPS, each group once
n=0
bt=$(for i in 1 2 3 4 5 6 7 8; do make -s --no-print-directory print-var VAR=BT_GROUPS_$i; done | tr ' ' '\n' | grep . | sort)
if [ "$bt" != "$(echo $groups | tr ' ' '\n' | sort)" ]; then
    echo "check_ci_groups: BT_GROUPS_N are not exactly TEST_GROUPS (a group missing or in two):"
    diff <(echo $groups | tr ' ' '\n' | sort) <(echo "$bt") | sed 's/^/  /' || true
    bad=1
fi
job_lines build-test | grep -q 'goals: "test TEST_GROUPS="' || { echo "check_ci_groups: the build-test job does not run \`make test TEST_GROUPS=\`"; bad=1; }
for t in $(make -s --no-print-directory print-var VAR=BT_TARGETS); do
    job_lines build-test | grep -qE "goals: \"?$t\"?[ ,}]" || { echo "check_ci_groups: $t is no goal of the build-test job"; bad=1; }
done
# jepsen: every part of the selftest the Makefile defines is in JEPSEN_SELFTEST_PARTS (what `make jepsen` runs) and in the matrix,
# and the runs are there on both event loops
parts=$(make -s --no-print-directory print-var VAR=JEPSEN_SELFTEST_PARTS)
for t in $(make -s --no-print-directory -qp 2>/dev/null | grep -oE '^jepsen-selftest-[0-9]+:' | tr -d :); do
    case " $parts " in *" $t "*) ;; *) echo "check_ci_groups: $t is defined and is not in JEPSEN_SELFTEST_PARTS"; bad=1 ;; esac
done
for t in $(make -s --no-print-directory print-var VAR=JEPSEN_SELFTEST_PARTS); do
    job_lines jepsen | grep -q "target: $t[ ,}]" || { echo "check_ci_groups: $t is no matrix target of the jepsen job"; bad=1; }
done
for io in uring epoll; do
    job_lines jepsen | grep -q "target: jepsen-run, io: $io[ ,}]" || { echo "check_ci_groups: jepsen-run on io: $io is no matrix entry of the jepsen job"; bad=1; }
done
# every matrix entry has a numeric timeout, and a job whose limit reads the matrix has a matrix (an empty one is the 360-minute default)
if grep -E '^ *- \{ name:' "$CI" | grep -vqE 'timeout: [0-9]+'; then
    echo "check_ci_groups: a matrix entry of ci.yml has no numeric timeout:"; grep -E '^ *- \{ name:' "$CI" | grep -vE 'timeout: [0-9]+' | sed 's/^/  /'; bad=1
fi
for j in $(awk '/^  [a-z0-9_-]+:/ { cur = $1; sub(":", "", cur); print cur }' "$CI"); do
    if job_lines "$j" | grep -q 'timeout-minutes:.*matrix\.' && ! job_lines "$j" | grep -q '^      matrix:'; then
        echo "check_ci_groups: job $j reads matrix.* for its timeout and has no matrix"; bad=1
    fi
done
[ $bad -eq 0 ] && echo "check_ci_groups: ok ($groups)"
exit $bad
