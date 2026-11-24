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

# BUILD MACHINES (se-remote.sh): every bgrun.sh job runs on one of them, the trees it names copied to the same paths there.
# One machine:
# SE_REMOTE=user@host
# SE_REMOTE_SLOTS=3                       # jobs at once there
# A fleet instead (SE_REMOTES wins when both are set): user@host:SLOTS[:tag[:tag]], tried in this order, the first with a
# free slot gets the job (none: the local overflow rule below, else the first host's queue). SLOTS empty = SE_REMOTE_SLOTS.
# SE_REMOTES="user@hostA:3 user@hostB:8:nvme user@hostC:8:nvme:ib"
# (SLOTS is 1-999, so user@host:2222 is refused; no valid entry at all makes bgrun.sh refuse, never run the job here.)
# Moving from SE_REMOTE to SE_REMOTES: replace the overflow block below in the SAME edit. An old block that probes "$SE_REMOTE"
# runs while this file is read, before the fleet is parsed: with SE_REMOTES only it probes nothing, sees 0 free slots, and every job
# overflows to this machine. Cost: the overflow rule probes every fitting host once and the pick probes them again (one ssh each, 15 s
# at most for a host that hangs, SE_PROBE_TIMEOUT).
# A job can ask for a tag or a host: BGRUN_TAG=nvme bgrun.sh ...   SE_HOST=user@hostB bgrun.sh ...
# Paths that are a host's own checkouts (never copied there; a job started in one goes to a host that does not own it):
# SE_REMOTE_OWN="$HOME/projects/git/myrepo"                      # every host
# SE_REMOTE_OWN_user_hostB="$HOME/projects/git/myrepo $HOME/x"   # this host only (user@hostB with _ for @ and .)
# SE_REMOTE_KEY=$HOME/.ssh/id_rsa         # ssh key for all of them
#
# Local overflow (a second machine, here): when no host has a free slot a job runs HERE, niced, at most SE_LOCAL_MAX at once,
# while the load is below SE_LOCAL_LOAD; `touch $SE_HOME/HOLD.local` keeps every job off this machine. se_free_total counts the
# free slots of the fleet (BGRUN_TAG and SE_HOST honoured); a job that asked for a tag or a host never overflows.
# SE_LOCAL_MAX=${SE_LOCAL_MAX:-10}
# SE_LOCAL_LOAD=${SE_LOCAL_LOAD:-24}
# if [ "${0##*/}" = bgrun.sh ] && [ -z "$BGRUN_LOCAL" ] && [ -z "$BGRUN_TAG$SE_HOST" ] && [ ! -e "$SE_HOME/HOLD.local" ]; then
#     se_fleet_parse; . "$SE_BIN/se-remote.sh"   # this file is read before the fleet is parsed
#     if [ "$(se_free_total)" = 0 ] && awk -v m="$SE_LOCAL_LOAD" '{exit !($1 < m)}' /proc/loadavg; then
#         _l=0
#         for _d in "$SE_HOME"/runs/*/; do
#             [ -f "$_d/rc" ] || [ -f "$_d/rpgid" ] && continue
#             _p=$(cat "$_d/pid" 2>/dev/null); [ -n "$_p" ] && kill -0 "$_p" 2>/dev/null && _l=$((_l+1))
#         done
#         [ "$_l" -lt "$SE_LOCAL_MAX" ] && BGRUN_LOCAL=1
#         unset _l _d _p
#     fi
# fi
# if [ "${0##*/}" = bgrun.sh ] && [ -n "$BGRUN_LOCAL" ]; then SE_QUEUE=$SE_HOME/nice-run.sh; fi   # your niced queue wrapper
