#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Tests the build fleet of the subagent-economy scripts (.claude/skills/subagent-economy/scripts: SE_REMOTES in config.sh,
# se-remote.sh): two fake hosts, `ssh` replaced by a stub that runs the command here with the host's own runs dir, slot
# files, git mirror and TMPDIR (the trees keep their absolute paths and are shared, as the real hosts get copies of them).
# A job writes $FAKE_HOST, which the stub exports, so what ran where is seen from the job itself, not from a file the
# scripts keep.
# Proves: the slot choice in config order and the claim of a job not yet in its slot (two jobs started in a row do not
# take the last slot of a host), the fallback to the first fitting host's queue, a host that is down, BGRUN_TAG and SE_HOST,
# per-host SE_REMOTE_OWN, the host recorded in runs/NAME/host and used by se-rjob (the job runs there, in that host's
# slots), rx.sh (this directory's host, -j, -H), await.sh and watchdog.sh (the job's host; every host asked for orphans),
# and a one-host config (SE_REMOTE) that behaves as before.
# --mutant NAME plants one bug in a copy of the scripts and runs the same checks: the run must FAIL (exit 1) at the
# first check that sees it (make test-se-fleet-mutants).
#
#   tests/test_se_fleet.sh [--mutant NAME] [scratch-dir]
set -u
top=$(cd "$(dirname "$0")/.." && pwd)
mutant=
[ "${1:-}" = --mutant ] && { mutant=$2; shift 2; }
scratch=$(mktemp -d "${1:-${TMPDIR:-/tmp}}/sefleet.XXXXXX") || exit 2
scratch=$(readlink -f "$scratch")
fail=0
cleanup() {
    local d p
    for d in "$scratch"/home/runs/*/; do
        for f in pid rpgid; do
            p=$(cat "$d/$f" 2>/dev/null)
            [[ "$p" =~ ^[0-9]+$ ]] && [ "$p" -gt 1 ] && kill -TERM -- "-$p" 2>/dev/null
        done
    done
    p=$(pgrep -u "$(id -u)" -x "sfo$$" | head -1)
    [[ "$p" =~ ^[0-9]+$ ]] && [ "$p" -gt 1 ] && kill "$p" 2>/dev/null
    rm -rf "$scratch"
}
trap cleanup EXIT
bad() {
    echo "FAIL: $*"
    fail=1
    # a planted bug is caught by its first failed check
    [ -n "$mutant" ] && { echo "mutant $mutant caught"; exit 1; }
}
ok() { echo "ok: $*"; }
eq() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got '$2', want '$3'"; fi; }
has() { if grep -q -- "$3" <<< "$2"; then ok "$1"; else bad "$1: no '$3' in: $(head -c 400 <<< "$2")"; fi; }
hasnt() { if grep -q -- "$3" <<< "$2"; then bad "$1: found '$3' in: $(head -c 400 <<< "$2")"; else ok "$1"; fi; }

# ---- the scripts under test (a copy with one planted bug for --mutant) ----
S=$top/.claude/skills/subagent-economy/scripts
if [ -n "$mutant" ]; then
    cp -r "$S" "$scratch/scripts" && S=$scratch/scripts
    # sub FILE OLD NEW: a literal replacement that must change the file
    sub() {
        python3 - "$S/$1" "$2" "$3" <<'EOF' || { echo "mutant $mutant does not apply: SURVIVED"; exit 0; }
import sys
p, old, new = sys.argv[1:4]
s = open(p).read()
if old not in s:
    sys.exit(1)
open(p, 'w').write(s.replace(old, new, 1))
EOF
    }
    case $mutant in
    nohost)       sub bgrun.sh '> $d/host' '> /dev/null' ;;
    rxdefault)    sub rx.sh 'h=$(se_cwd_host "$PWD")' 'h=${SE_FH[0]}' ;;
    rxjob)        sub rx.sh 'h=$(cat "$RUNS/$2/host" 2>/dev/null) ||' 'h=${SE_FH[0]}; : ||' ;;
    overbook)     sub se-remote.sh 'f=$(se_probe_free "$h" "${SE_FS[i]}"); f=$(( ${f:-0} - $(se_claims "$h") ))' 'f=$(se_probe_free "$h" "${SE_FS[i]}"); f=$(( ${f:-0} ))' ;;
    noprobe)      sub se-remote.sh 'f=$(se_probe_free "$h" "${SE_FS[i]}"); f=$(( ${f:-0} -' 'f=99; f=$(( ${f:-0} -' ;;
    downfree)     sub se-remote.sh 'f=$(se_probe_free "$h" "${SE_FS[i]}"); f=$(( ${f:-0} -' 'f=$(se_probe_free "$h" "${SE_FS[i]}"); f=$(( ${f:-9} -' ;;
    fallbacklast) sub se-remote.sh 'echo "${SE_FH[${c[0]}]}"; return 1' 'echo "${SE_FH[${c[-1]}]}"; return 1' ;;
    slotsignored) sub se-remote.sh 'SE_REMOTE_SLOTS=${SE_FS[i]}' ':' ;;
    rjobhost)     sub se-rjob.sh '[ -z "$RJ_HOST" ] ||' 'true ||' ;;
    notag)        sub se-remote.sh '[ -z "${BGRUN_TAG:-}" ] ||' 'true ||' ;;
    ownglobal)    sub se-remote.sh 'if [ -n "${!v+x}" ]' 'if false' ;;
    awaithost)    sub await.sh 'hl=" on $(cat $d/host)"' 'hl=' ;;
    awaitone)     sub await.sh '[ ${#SE_FH[@]} -gt 1 ] && [ -s' '[ ${#SE_FH[@]} -gt 0 ] && [ -s' ;;
    wdfirst)      sub watchdog.sh 'for h in "${SE_FH[@]}"; do' 'for h in "${SE_FH[0]}"; do' ;;
    wdhost)       sub watchdog.sh 'rh=$(cat "$d/host" 2>/dev/null); rh=${rh:-$SE_REMOTE}' 'rh=$SE_REMOTE' ;;
    overflow)     sub se-remote.sh 'f=$(se_probe_free "${SE_FH[i]}" "${SE_FS[i]}"); f=$(( ${f:-0} - $(se_claims "${SE_FH[i]}") ))' 'f=$(se_probe_free "${SE_FH[i]}" "${SE_FS[i]}"); f=$(( ${f:-0} ))' ;;
    wdone)        sub watchdog.sh 'suffix=; [ ${#SE_FH[@]} -gt 1 ]' 'suffix=; [ ${#SE_FH[@]} -gt 0 ]' ;;
    pushfirst)    sub se-remote.sh '"$r/" "$SE_REMOTE:$r/" ||' '"$r/" "${SE_FH[0]}:$r/" ||' ;;
    backfirst)    sub se-remote.sh '"$SE_REMOTE:$r/" "$r/" ||' '"${SE_FH[0]}:$r/" "$r/" ||' ;;
    gitfirst)     sub se-remote.sh '"$common/" "$SE_REMOTE:$rc/"' '"$common/" "${SE_FH[0]}:$rc/"' ;;
    nolock)       sub bgrun.sh 'flock -w 60 7 ||' 'true ||' ;;
    pinlock)      sub bgrun.sh 'if [ -z "$BGRUN_NOWAIT" ] && [ "$(se_cands "$PWD" | wc -l)" -gt 1 ]; then' 'if true; then' ;;
    noprobetimeout) sub se-remote.sh 'timeout "${SE_PROBE_TIMEOUT:-15}" ssh' 'ssh' ;;
    claimneedspid) sub se-remote.sh 'if [ -n "$p" ]; then kill -0' 'if true; then kill -0' ;;
    claimslot)    sub se-remote.sh "grep -q '^== build machine slot' \"\$d/out\" 2>/dev/null && continue" ':' ;;
    claimnowait)  sub se-remote.sh ' && [ ! -e "$d/nowait" ]' '' ;;
    nowaitprobe)  sub se-remote.sh 'if [ -n "${BGRUN_NOWAIT:-}" ] || [ ${#c[@]} -eq 1 ]' 'if [ ${#c[@]} -eq 1 ]' ;;
    rrunsticky)   sub rrun.sh 'host=$(se_cwd_host "$PWD")' 'host=' ;;
    rrunnonote)   sub rrun.sh 'se_cwd_note "$PWD" "$host"' ':' ;;
    endnote)      sub se-rjob.sh 'rx.sh -j $RJ_N CMD' 'rx.sh CMD' ;;
    badlocal)     sub bgrun.sh 'if [ -n "${SE_REMOTES:-}" ] && ! se_remote_on' 'if false && ! se_remote_on' ;;
    slotcap)      sub se-env.sh '{0,2}' '*' ;;
    ownmsg)       sub se-remote.sh 'if [ ${#c[@]} -eq 0 ] && [ -n "${1:-}" ] &&' 'if false &&' ;;
    rbnolock)     sub rbworker.sh 'flock -n 9' 'true' ;;
    *) echo "unknown mutant $mutant"; exit 2 ;;
    esac
fi

# ---- the fake hosts ----
# nothing of the caller's environment (a build machine's SE_REMOTES, a BGRUN_LOCAL, an SE_HOME of its own) may reach the scripts
for v in $(compgen -e | grep -E '^(SE_|BGRUN_|RB_|RX_|WATCHDOG_|FAKE_)'); do unset "$v"; done
export SE_HOME=$scratch/home SE_REMOTE_RUNS=$scratch/rruns SE_REMOTE_GIT=$scratch/rgit SE_RJOB_POLL=1
export SE_REMOTE_PREFIXES="$scratch:1" FAKE_ROOT=$scratch
mkdir -p "$SE_HOME" "$scratch/stub"
touch "$SE_HOME/BENCH.lock"
cat > "$scratch/stub/ssh" <<'EOF'
#!/bin/bash
# a fake ssh: the command runs here, in the host's own state dirs ($FAKE_ROOT/hosts/<host>), with FAKE_HOST set
# (rsync starts its ssh as `ssh -l user host`: the host is user@host in both forms)
user=
while [ $# -gt 0 ]; do case "$1" in -l) user=$2; shift 2 ;; -i | -o | -p) shift 2 ;; -*) shift ;; *) break ;; esac; done
host=$1; shift
[[ "$host" == *@* ]] || host=${user:+$user@}$host
cmd="$*"
san=${host//[^A-Za-z0-9]/_}
[ -e "$FAKE_ROOT/down.$san" ] && exit 255
# a host that accepts the connection and hangs on the slot probe (a stuck mount); stdout closed so a killed stub leaves no writer
[ -e "$FAKE_ROOT/hang.$san" ] && [[ "$cmd" == *"flock -n 9"* ]] && sleep 12 > /dev/null 2>&1
hd=$FAKE_ROOT/hosts/$san; mkdir -p "$hd"
cmd=${cmd//"$SE_REMOTE_RUNS"/$hd/rruns}
cmd=${cmd//"$SE_REMOTE_GIT"/$hd/rgit}
cmd=${cmd//\/dev\/shm\/iso-ci.lock/$hd/rb.lock}
cmd=${cmd//\/dev\/shm\/iso-ci/$hd/rbroot}
cmd=${cmd//\/dev\/shm\/t/$hd/shm_t}
# the host's own /var/tmp/t and /var/tmp/j; the scratch dir of this test can itself lie under them (TMPDIR of a job
# run by bgrun.sh is /var/tmp/t/NAME), so what lies under it is kept out of the rewrite
cmd=${cmd//"$FAKE_ROOT"/@@FAKE_ROOT@@}
cmd=${cmd//\/var\/tmp\/t/$hd/t}
cmd=${cmd//\/var\/tmp\/j/$hd/j}
cmd=${cmd//@@FAKE_ROOT@@/$FAKE_ROOT}
printf '%s\t%s\n' "$host" "${cmd:0:100}" >> "$FAKE_ROOT/ssh.log"
export FAKE_HOST=$host
cd "$HOME" && exec bash -c "$cmd"
EOF
chmod +x "$scratch/stub/ssh"
export PATH=$scratch/stub:$PATH
A=user@hostA B=user@hostB
sanA=user_hostA sanB=user_hostB

conf() { printf '%s\n' "$@" > "$SE_HOME/config.sh"; }
# job NAME DIR CMD: start a detached job in DIR (a job writes where.NAME: its host and start and end times)
jobcmd() { echo "echo \${FAKE_HOST:-local} \$(date +%s) > where.$1; sleep ${2:-8}; date +%s >> where.$1"; }
start() { local n=$1 d=$2; shift 2; mkdir -p "$scratch/$d"; (cd "$scratch/$d" && "$S/bgrun.sh" "$n" bash -c "$(jobcmd "$n" "$@")" 2>&1); }
hostof() { cat "$SE_HOME/runs/$1/host" 2>/dev/null; }
ranon() { head -1 "$scratch/$2/where.$1" 2>/dev/null | cut -d' ' -f1; }
wait_rc() { local i; for i in $(seq 1 400); do [ -e "$SE_HOME/runs/$1/rc" ] && return 0; sleep 0.5; done; bad "job $1 did not finish in 200 s"; }
rcof() { cat "$SE_HOME/runs/$1/rc" 2>/dev/null; }

# ---- a fleet: A has 1 slot, B has 2 and the tag nvme ----
conf "SE_REMOTES=\"$A:1 $B:2:nvme\""
out=$(start j1 job1)
has "j1 starts" "$out" "started j1 on the build machine $A"
eq "j1 goes to the first host" "$(hostof j1)" "$A"
out=$(start j2 job2)
eq "j2 goes to B (A's last slot is claimed by j1, which has not copied its trees yet)" "$(hostof j2)" "$B"
start j3 job3 > /dev/null
eq "j3 goes to B (its second slot)" "$(hostof j3)" "$B"
out=$(start j4 job4)
eq "j4: no host has a slot: the first host's queue" "$(hostof j4)" "$A"
has "j4 says it waits" "$out" "waits in the queue of $A"
for n in j1 j2 j3 j4; do wait_rc $n; done
for n in j1 j2 j3 j4; do eq "$n exit code" "$(rcof $n)" 0; done
eq "j1 ran on A" "$(ranon j1 job1)" "$A"
eq "j2 ran on B" "$(ranon j2 job2)" "$B"
eq "j3 ran on B" "$(ranon j3 job3)" "$B"
eq "j4 ran on A, where it queued, not on B" "$(ranon j4 job4)" "$A"
s4=$(sed -n 1p "$scratch/job4/where.j4" | cut -d' ' -f2); e1=$(sed -n 2p "$scratch/job1/where.j1")
[ -n "$s4" ] && [ -n "$e1" ] && [ "$s4" -ge "$e1" ] && ok "j4 waited for A's one slot (started $s4, j1 ended $e1)" || bad "A ran two jobs at once with 1 slot (j4 started $s4, j1 ended $e1)"
[ -e "$scratch/hosts/$sanA/rruns/j1/rc" ] && [ ! -e "$scratch/hosts/$sanB/rruns/j1/rc" ] && ok "j1's run dir is on A only" || bad "j1's run dir is not on A only"
[ -e "$scratch/hosts/$sanB/rruns/j2/rc" ] && [ ! -e "$scratch/hosts/$sanA/rruns/j2/rc" ] && ok "j2's run dir is on B only" || bad "j2's run dir is not on B only"
eq "j2's cwd noted" "$(cat "$SE_HOME/runs/j2/cwd")" "$scratch/job2"

# await names the job's host
has "await: DONE on the job's host" "$("$S/await.sh" j2 30)" "^DONE j2 on $B rc=0"
has "await: j1 on A" "$("$S/await.sh" j1 30)" "^DONE j1 on $A rc=0"

# rx.sh: this directory's host, a directory below it, -j, -H
eq "rx in j2's directory: B" "$(cd "$scratch/job2" && "$S/rx.sh" printenv FAKE_HOST)" "$B"
eq "rx in j1's directory: A" "$(cd "$scratch/job1" && "$S/rx.sh" printenv FAKE_HOST)" "$A"
mkdir -p "$scratch/job2/sub"
eq "rx below j2's directory: B" "$(cd "$scratch/job2/sub" && "$S/rx.sh" printenv FAKE_HOST)" "$B"
eq "rx -j j3: B" "$(cd "$scratch" && "$S/rx.sh" -j j3 printenv FAKE_HOST)" "$B"
eq "rx -j j4: A" "$(cd "$scratch" && "$S/rx.sh" -j j4 printenv FAKE_HOST)" "$A"
eq "rx -H A" "$(cd "$scratch" && "$S/rx.sh" -H $A printenv FAKE_HOST)" "$A"
mkdir -p "$scratch/nojob"
out=$(cd "$scratch/nojob" && "$S/rx.sh" printenv FAKE_HOST 2>&1); rc=$?
eq "rx where no job ran, with two hosts: refused" "$rc" 2
has "rx names the hosts" "$out" "$A $B"

# rrun.sh: sticky to the directory's host (the incremental build is there), re-picks when that host does not fit, and rx follows
eq "rrun in j2's directory: B, though A is free (and first)" "$(cd "$scratch/job2" && "$S/rrun.sh" printenv FAKE_HOST)" "$B"
eq "rrun in j1's directory: A" "$(cd "$scratch/job1" && "$S/rrun.sh" printenv FAKE_HOST)" "$A"
mkdir -p "$scratch/rr1" "$scratch/rr2"
eq "rrun with SE_HOST=B in a new directory" "$(cd "$scratch/rr1" && SE_HOST=$B "$S/rrun.sh" printenv FAKE_HOST)" "$B"
eq "rx in that directory follows rrun" "$(cd "$scratch/rr1" && "$S/rx.sh" printenv FAKE_HOST)" "$B"
eq "rrun on A first" "$(cd "$scratch/rr2" && SE_HOST=$A "$S/rrun.sh" printenv FAKE_HOST)" "$A"
eq "rrun with the tag nvme re-picks: A has no such tag" "$(cd "$scratch/rr2" && BGRUN_TAG=nvme "$S/rrun.sh" printenv FAKE_HOST)" "$B"
eq "rx in that directory follows the re-pick" "$(cd "$scratch/rr2" && "$S/rx.sh" printenv FAKE_HOST)" "$B"

# the end note of a job names the job: rx.sh without -j reads the last job of the directory, not this one
has "j1's end note names its job for rx.sh" "$(cat "$SE_HOME/runs/j1/out")" "rx.sh -j j1 CMD"
has "j1's end note names its host" "$(cat "$SE_HOME/runs/j1/out")" "ran on the build machine ($A)"

# tags and hosts
out=$(BGRUN_TAG=nvme start j5 job5 4)
eq "BGRUN_TAG=nvme: B though A is free" "$(hostof j5)" "$B"
out=$(SE_HOST=$A start j6 job6 4)
eq "SE_HOST=A" "$(hostof j6)" "$A"
out=$(BGRUN_TAG=nosuch start j7 job7 4); has "an unknown tag is refused" "$out" "no build host fits tag nosuch"
[ ! -s "$scratch/job7/where.j7" ] && ok "nothing ran for the unknown tag" || bad "a job ran for the unknown tag"
for n in j5 j6; do wait_rc $n; done
eq "j5 ran on B" "$(ranon j5 job5)" "$B"
eq "j6 ran on A" "$(ranon j6 job6)" "$A"

# a host that is down is skipped
touch "$scratch/down.$sanA"
start j8 job8 3 > /dev/null
eq "A is down: j8 goes to B" "$(hostof j8)" "$B"
wait_rc j8; eq "j8 ran on B" "$(ranon j8 job8)" "$B"
rm -f "$scratch/down.$sanA"

# SE_REMOTE_OWN per host: a checkout that is A's own is not a place for a job on A, B takes it
mkdir -p "$scratch/ownA" "$scratch/ownG"
conf "SE_REMOTES=\"$A:1 $B:2:nvme\"" "SE_REMOTE_OWN=$scratch/ownG" "SE_REMOTE_OWN_user_hostA=\"$scratch/ownA $scratch/ownG\""
start j9 ownA 3 > /dev/null
eq "j9 in A's own checkout: B" "$(hostof j9)" "$B"
out=$(start j10 ownG 3); has "j10 in a checkout both hosts own (A's list and the default of B): refused, with the old words" "$out" "own checkout"
wait_rc j9; eq "j9 ran on B" "$(ranon j9 ownA)" "$B"

# watchdog: the job on B is judged on B, every host is asked for orphans
cat > "$scratch/stub/sfo$$" <<'EOF'
#!/bin/bash
while :; do sleep 1; done
EOF
chmod +x "$scratch/stub/sfo$$"
conf "SE_REMOTES=\"$A:1 $B:2:nvme\""
export WATCHDOG_ORPHANS=sfo$$
( setsid nohup "$scratch/stub/sfo$$" --dir "$scratch/orph" --port-file "$scratch/pf" > /dev/null 2>&1 & )
sleep 1
BGRUN_TAG=nvme start j11 job11 25 > /dev/null
for i in $(seq 1 60); do [ -s "$SE_HOME/runs/j11/rpgid" ] && break; sleep 0.5; done
wd=$("$S/watchdog.sh" 45 2>&1)
has "watchdog: j11 judged on its host" "$wd" "^RUNNING  j11  (build machine $B) "
hasnt "watchdog: not on A" "$wd" "j11  (build machine $A)"
if grep -q '^LOOK orphan sfo' <<< "$wd"; then
    eq "watchdog asks A for orphans" "$(grep -c "^LOOK remote orphan sfo.* on $A\$" <<< "$wd")" 1
    eq "watchdog asks B for orphans" "$(grep -c "^LOOK remote orphan sfo.* on $B\$" <<< "$wd")" 1
    orph=1
else
    echo "skipped: the orphan checks (a server that lost its parent is not reparented to init or a user's systemd here)"; orph=
fi
wait_rc j11; eq "j11 ran on B" "$(ranon j11 job11)" "$B"

# local overflow: the rule of config.example.sh (se_free_total) sends a job HERE when no host has a free slot
conf "SE_REMOTES=\"$A:1 $B:1\"" 'SE_LOCAL_LOAD=100000' \
    'if [ "${0##*/}" = bgrun.sh ] && [ -z "$BGRUN_LOCAL" ] && [ -z "$BGRUN_TAG$SE_HOST" ]; then' \
    '    se_fleet_parse; . "$SE_BIN/se-remote.sh"' \
    '    [ "$(se_free_total)" = 0 ] && awk -v m="$SE_LOCAL_LOAD" '"'"'{exit !($1 < m)}'"'"' /proc/loadavg && BGRUN_LOCAL=1' \
    'fi'
start x1 ov1 4 > /dev/null
start x2 ov2 4 > /dev/null
start x3 ov3 2 > /dev/null
eq "overflow: x1 on A" "$(hostof x1)" "$A"
eq "overflow: x2 on B" "$(hostof x2)" "$B"
eq "overflow: x3 has no host (it runs here)" "$(hostof x3)" ""
for n in x1 x2 x3; do wait_rc $n; done
eq "overflow: x3 ran here" "$(ranon x3 ov3)" "local"
[ -s "$scratch/ov3/where.x3" ] && ok "overflow: x3 ran" || bad "overflow: x3 did not run"
eq "overflow: x1 ran on A" "$(ranon x1 ov1)" "$A"

# ---- where the files go: pushed to the picked host only, pulled back from it only, its git mirror ----
conf "SE_REMOTES=\"$A:1 $B:2\""
git init -q "$scratch/repo" && git -C "$scratch/repo" -c user.name=t -c user.email=t@t commit -q --allow-empty -m x &&
    git -C "$scratch/repo" worktree add -q "$scratch/wt" 2>/dev/null
: > "$scratch/ssh.log"
SE_HOST=$B start g1 wt 2 > /dev/null; wait_rc g1
eq "g1 (SE_HOST=B, the second host) ran on B" "$(ranon g1 wt)" "$B"
eq "the trees were pushed to B only" "$(grep 'rsync --server' "$scratch/ssh.log" | grep -v -- '--sender' | cut -f1 | sort -u)" "$B"
eq "what the job wrote and its out came back from B only" "$(grep 'rsync --server --sender' "$scratch/ssh.log" | cut -f1 | sort -u)" "$B"
[ -d "$scratch/repo/.git/worktrees/wt" ] && ok "the worktree is real" || bad "the test worktree was not made"
eq "the git mirror of the worktree is on B" "$(ls "$scratch/hosts/$sanB/rgit" 2>/dev/null | wc -l)" 1
eq "and not on A" "$(ls "$scratch/hosts/$sanA/rgit" 2>/dev/null | wc -l)" 0

# ---- two jobs started at the same moment take two hosts (the pick and its record are one step under the lock) ----
conf "SE_REMOTES=\"$A:1 $B:1\""
same=0
for k in 1 2 3 4; do
    start c${k}a cc${k}a 2 > /dev/null & start c${k}b cc${k}b 2 > /dev/null & wait
    [ "$(hostof c${k}a)" != "$(hostof c${k}b)" ] || same=$((same + 1))
    wait_rc c${k}a; wait_rc c${k}b
done
eq "4 rounds of two concurrent jobs on two 1-slot hosts: none shared a host" "$same" 0

# se_claims itself: a job not yet in its slot is a claim, with a pid that lives or (a minute at most) with none yet
cl=$scratch/cl; mkdir -p "$cl"
mkcl() { mkdir -p "$cl/$1"; printf '%s\n' "$2" > "$cl/$1/host"; date +%s > "$cl/$1/start"; }
mkcl nopid $A                                                   # bgrun between the lock and its pid file: counts
mkcl old $A; echo $(( $(date +%s) - 100 )) > "$cl/old/start"    # no pid for 100 s: bgrun died, no claim
mkcl dead $A; sleep 0 & dp=$!; wait $dp; echo $dp > "$cl/dead/pid"
mkcl live $A; echo $$ > "$cl/live/pid"
mkcl done $A; echo $$ > "$cl/done/pid"; echo 0 > "$cl/done/rc"
mkcl nw $A; echo $$ > "$cl/nw/pid"; : > "$cl/nw/nowait"
mkcl inslot $A; echo $$ > "$cl/inslot/pid"; echo "== build machine slot 1, 12:00:00" > "$cl/inslot/out"
mkcl other $B; echo $$ > "$cl/other/pid"
eq "claims on A: the job without a pid yet and the live one, nothing else" "$( ( RUNS=$cl; . "$S/se-env.sh"; . "$S/se-remote.sh"; se_claims $A ) )" 2

# ---- a NOWAIT job takes the first fitting host with no probe and no slot; it is no claim ----
start w0 nw0 8 > /dev/null
for i in $(seq 1 60); do grep -q '^== build machine slot' "$SE_HOME/runs/w0/out" 2>/dev/null && break; sleep 0.5; done
eq "w0 holds A's slot, and is no claim any more" "$( ( . "$S/se-env.sh"; . "$S/se-remote.sh"; se_claims $A ) )" 0
BGRUN_NOWAIT=1 start w1 nw1 2 > /dev/null
eq "NOWAIT: the first fitting host, though A's slot is taken" "$(hostof w1)" "$A"
start w2 nw2 2 > /dev/null
eq "a normal job after it: B (A's slot is taken)" "$(hostof w2)" "$B"
for n in w0 w1 w2; do wait_rc $n; done
BGRUN_NOWAIT=1 start w3 nw3 3 > /dev/null
start w4 nw4 2 > /dev/null
eq "a NOWAIT job holds no slot and claims none: the next job takes A" "$(hostof w4)" "$A"
for n in w3 w4; do wait_rc $n; done

# ---- a host that hangs (not fails) in the probe delays one pick, never the starts of jobs that need no probe ----
export SE_PROBE_TIMEOUT=6
conf "SE_REMOTES=\"$A:1 $B:2\""
touch "$scratch/hang.$sanA"
start hp1 hp1 2 > /dev/null &
sleep 0.5
t0=$(date +%s.%N); SE_HOST=$B start hp2 hp2 2 > /dev/null; t1=$(date +%s.%N)
el=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%d", (b - a) * 10 }')
[ "$el" -lt 40 ] && ok "a job pinned to B started in $el/10 s while the pick of another waits for A's hung probe" || bad "a job pinned to B waited $el/10 s behind a hung probe"
wait
eq "the probe of the hung A timed out: hp1 went to B" "$(hostof hp1)" "$B"
rm -f "$scratch/hang.$sanA"; unset SE_PROBE_TIMEOUT
wait_rc hp1; wait_rc hp2

# ---- a bad SE_REMOTES never sends a job to run here, un-niced ----
conf "SE_REMOTES=\"$A:2222\""
out=$(start b1 bad1 1 2>&1); rc=$(rcof b1)
has "SE_REMOTES with no valid entry (a port is not slots): refused" "$out" "no valid entry"
eq "refused with 2" "$rc" 2
[ ! -s "$scratch/bad1/where.b1" ] && ok "nothing ran" || bad "the job ran although no host is valid"
out=$(BGRUN_LOCAL=1 start b2 bad2 1 2>&1); wait_rc b2
eq "BGRUN_LOCAL=1 still runs it here" "$(ranon b2 bad2)" "local"

# ---- one host, a job from the host's own checkout: the old words ----
conf "SE_REMOTE=$A" "SE_REMOTE_OWN=$scratch/ownA"
out=$(start o0 ownA 1 2>&1)
has "one host, own checkout: says so" "$out" "own checkout"
eq "refused with 2" "$(rcof o0)" 2

# ---- one host, as before: SE_REMOTE and SE_REMOTE_SLOTS ----
: > "$scratch/ssh.log"
conf "SE_REMOTE=$A" "SE_REMOTE_SLOTS=1"
start o1 one1 6 > /dev/null
start o2 one2 3 > /dev/null
eq "one host: o1 on A" "$(hostof o1)" "$A"
eq "one host: o2 queues on A" "$(hostof o2)" "$A"
wait_rc o1; wait_rc o2
eq "one host: o1 ran on A" "$(ranon o1 one1)" "$A"
eq "one host: o2 ran on A" "$(ranon o2 one2)" "$A"
s2=$(sed -n 1p "$scratch/one2/where.o2" | cut -d' ' -f2); e1=$(sed -n 2p "$scratch/one1/where.o1")
[ -n "$s2" ] && [ -n "$e1" ] && [ "$s2" -ge "$e1" ] && ok "one host: SE_REMOTE_SLOTS=1 held (o2 started $s2, o1 ended $e1)" || bad "one host: two jobs at once with 1 slot"
hasnt "one host: no slot probe" "$(cat "$scratch/ssh.log")" "flock -n 9"
a=$("$S/await.sh" o1 30)
has "one host: await as before" "$a" "^DONE o1 rc=0 after"
eq "one host: rx in a directory with no job" "$(cd "$scratch/nojob" && "$S/rx.sh" printenv FAKE_HOST)" "$A"
eq "one host: rx in the job's directory" "$(cd "$scratch/one1" && "$S/rx.sh" printenv FAKE_HOST)" "$A"
wd=$("$S/watchdog.sh" 45 2>&1)
if [ -n "$orph" ]; then
    eq "one host: the orphan line as before" "$(grep -c '^LOOK remote orphan sfo' <<< "$wd")" 1
    hasnt "one host: no host at its end" "$wd" "^LOOK remote orphan.* on user"
fi

# ---- rbgrun.sh / rbworker.sh: flock per host (even when mkdir always returns 0, as uutils coreutils 0.10 can on a race) ----
cat > "$scratch/stub/sudo" <<'EOF'
#!/bin/bash
exec "$@"
EOF
cat > "$scratch/stub/prlimit" <<'EOF'
#!/bin/bash
while [ $# -gt 0 ]; do case "$1" in --nofile=*|--memlock=*) shift ;; *) break ;; esac; done
exec "$@"
EOF
chmod +x "$scratch/stub/sudo" "$scratch/stub/prlimit"
mkdir -p "$scratch/rbwt" "$scratch/mkshim"
echo hello > "$scratch/rbwt/hello"
cat > "$scratch/mkshim/mkdir" <<'EOF'
#!/bin/bash
/bin/mkdir -p "$@"
exit 0
EOF
chmod +x "$scratch/mkshim/mkdir"

export RB_POLL=1
# Two racing callers on one host under the mkdir=0 shim must never hold the host at the same time
conf "RB_HOSTS=\"$A\""
rbjob() {
    local n=$1 d=${2:-2}
    echo "touch '$scratch/rb_act.$n'; ls '$scratch'/rb_act.* 2>/dev/null | wc -l >> '$scratch/rb_conc.log'; sleep $d; ls '$scratch'/rb_act.* 2>/dev/null | wc -l >> '$scratch/rb_conc.log'; rm -f '$scratch/rb_act.$n'; echo \${FAKE_HOST:-none} > '$scratch/rb_ran.$n'"
}
PATH=$scratch/mkshim:$PATH "$S/rbgrun.sh" rb1 "$scratch/rbwt" - bash -c "$(rbjob rb1 2)" > /dev/null &
PATH=$scratch/mkshim:$PATH "$S/rbgrun.sh" rb2 "$scratch/rbwt" - bash -c "$(rbjob rb2 2)" > /dev/null &
wait
wait_rc rb1; wait_rc rb2
eq "rb1 exit code" "$(rcof rb1)" 0
eq "rb2 exit code" "$(rcof rb2)" 0
eq "rb1 ran on A" "$(cat "$scratch/rb_ran.rb1" 2>/dev/null)" "$A"
eq "rb2 ran on A" "$(cat "$scratch/rb_ran.rb2" 2>/dev/null)" "$A"
rbmax=$(sort -n "$scratch/rb_conc.log" 2>/dev/null | tail -1)
eq "rbgrun: two racing callers under mkdir=0 shim never ran at once (max active)" "$rbmax" 1

# Two racing callers on two hosts take different hosts
conf "RB_HOSTS=\"$A $B\""
"$S/rbgrun.sh" rb3 "$scratch/rbwt" - bash -c "echo \${FAKE_HOST:-none} > '$scratch/rb_ran.rb3'; sleep 2" > /dev/null &
"$S/rbgrun.sh" rb4 "$scratch/rbwt" - bash -c "echo \${FAKE_HOST:-none} > '$scratch/rb_ran.rb4'; sleep 2" > /dev/null &
wait
wait_rc rb3; wait_rc rb4
eq "rb3 exit code" "$(rcof rb3)" 0
eq "rb4 exit code" "$(rcof rb4)" 0
[ "$(hostof rb3)" != "$(hostof rb4)" ] && ok "rb3 and rb4 took different hosts ($(hostof rb3) vs $(hostof rb4))" || bad "rb3 and rb4 shared host $(hostof rb3)"

# SIGKILL of rbworker.sh releases the flock immediately (no stale lock)
conf "RB_HOSTS=\"$A\""
"$S/rbgrun.sh" rb5 "$scratch/rbwt" - bash -c "echo \$\$ > '$scratch/rb5.child'; touch '$scratch/rb5.up'; exec sleep 15" > /dev/null
for i in $(seq 1 60); do [ -e "$scratch/rb5.up" ] && break; sleep 0.1; done
[ -e "$scratch/rb5.up" ] && ok "rb5 started" || bad "rb5 did not start"
wp=$(cat "$SE_HOME/runs/rb5/pid" 2>/dev/null)
kill -KILL "$wp" 2>/dev/null
for i in $(seq 1 40); do kill -0 "$wp" 2>/dev/null || break; sleep 0.1; done
"$S/rbgrun.sh" rb6 "$scratch/rbwt" - bash -c "echo ok > '$scratch/rb6.ok'" > /dev/null
wait_rc rb6
eq "rb6 acquired A after SIGKILL of rb5" "$(rcof rb6)" 0
eq "rb6 ran" "$(cat "$scratch/rb6.ok" 2>/dev/null)" "ok"
cp=$(cat "$scratch/rb5.child" 2>/dev/null)
[[ "$cp" =~ ^[0-9]+$ ]] && [ "$cp" -gt 1 ] && kill -KILL "$cp" 2>/dev/null || true

if [ $fail = 0 ]; then
    [ -n "$mutant" ] && { echo "mutant $mutant SURVIVED"; exit 0; }
    echo "ok: the build fleet"
fi
exit $fail
