# SPDX-License-Identifier: AGPL-3.0-only
# Sourced by the CLI tests (test_cli_input.sh, test_cli_latest.sh, ...). A "refused" check must see a refusal, not
# any failure: the CLI refuses with status 1 (a runtime or parse error) or 2 (a usage error), and always says why on
# stderr ("tsdb: ..." or the usage text). A crash (139), a timeout (124), a missing binary (127) or a refusal with no
# message is a bug, never "the input was refused". Each refusal family also needs a positive control in the test: the
# same command with a valid value succeeds, so a typo in the command cannot pass for a refusal.
#
#   cli_refusal_ok RC STDERR_FILE WORD      status 0 when RC is 1 or 2 and STDERR_FILE contains WORD
cli_refusal_ok() {
    local rc=$1 errf=$2 word=$3
    { [ "$rc" -eq 1 ] || [ "$rc" -eq 2 ]; } || return 1
    grep -qF -- "$word" "$errf"
}
