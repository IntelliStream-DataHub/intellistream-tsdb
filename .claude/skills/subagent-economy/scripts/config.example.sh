# SPDX-License-Identifier: AGPL-3.0-only
# Copy to $SE_HOME/config.sh (default ~/.local/state/subagent-economy/config.sh) and keep what this machine needs.

# A queue wrapper for heavy local jobs: a script that waits for a free slot on the machine, then execs its arguments.
# Unset = bgrun.sh runs the command at once.
# SE_QUEUE=/path/to/waitrun.sh

# The orchestrating session whose usage gates the waiters (save-usage.sh writes one file per session plus usage.json
# for the latest one). Unset = the latest reading of any session, which is the same account.
# USAGE_SESSION=<the orchestrating session's id>

# The fleet for rbgrun.sh (only needed when you have one).
# RB_HOSTS="user@host1 user@host2"
# RB_KEY=$HOME/.ssh/id_rsa
# RB_MIRROR_ROOTS="$SE_HOME /path/to/your/repo"   # the only trees mirror mode may copy (and delete on the machine)
# RB_REPO=/path/to/your/repo                                    # whose .git RB_GIT=1 ships along
# RB_BUSY='myserverd|otherd'                                      # skip a machine running any of these (pgrep -x)
# RB_REMOTE_ENV="PKG_CONFIG_PATH=/usr/local/lib/pkgconfig"
