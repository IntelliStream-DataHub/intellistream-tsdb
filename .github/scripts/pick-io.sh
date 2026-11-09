#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Prints the event loop backends the server tests can use here, for `make test IO="$(pick-io.sh)"`:
# "uring epoll" when this machine lets the current user set up an io_uring, otherwise "epoll".
#
#   .github/scripts/pick-io.sh
#
# Why: `make test` runs the server tests with `--io uring` and `--io epoll`, and `--io uring` is an error where the
# kernel or its policy (kernel.io_uring_disabled, a container's seccomp profile) refuses io_uring, as on RHEL and
# AlmaLinux 10. The integration harness (tests/integration/run.py) has its own probe and skips uring by itself.
# The probe calls io_uring_setup (syscall 425) the way tsdbd does, so nothing has to be built first.
# The answer goes to stdout; the reason goes to stderr (and, on GitHub, as a warning annotation).
set -eu
if python3 -I - <<'EOF'
import ctypes, os, sys
libc = ctypes.CDLL(None, use_errno=True)
params = ctypes.create_string_buffer(120)  # struct io_uring_params, zeroed
fd = libc.syscall(425, 4, params)           # io_uring_setup(4 entries, &params)
if fd >= 0:
    os.close(fd)
    sys.exit(0)
sys.stderr.write("io_uring_setup failed: %s\n" % os.strerror(ctypes.get_errno()))
sys.exit(1)
EOF
then
    echo "uring epoll"
else
    echo "pick-io: io_uring is not available here, the server tests run on epoll only" >&2
    [ -z "${GITHUB_ACTIONS:-}" ] || echo "::warning::io_uring is not available on this runner: server tests run on epoll only" >&2
    echo "epoll"
fi
