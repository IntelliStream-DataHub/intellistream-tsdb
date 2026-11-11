#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd that never starts (it says why and exits at once), for the jepsen self-test: an outage run that ends there
# must still keep the server's log, as it does at any other end of a failed run.
echo "dead-tsdbd: refusing to start" >&2
exit 3
