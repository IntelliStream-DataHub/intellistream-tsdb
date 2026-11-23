#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd that never starts (it says why, as a machine out of resources does, and exits at once), for the jepsen self-test: an outage
# run that ends there (a harness error: the environment) must still keep the server's log, as it does at any other end of a failed run.
# (A server that exits at open and names no failure of the machine is a verdict, exit 1: round 14.)
echo "dead-tsdbd: refusing to start: io_uring: Cannot allocate memory" >&2
exit 3
