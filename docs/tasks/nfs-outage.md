# Task: take NFS storage away from tsdbd on a real server and watch what it does

Step N1 of the NFS outage work ([docs/ha.md](../ha.md) section 15, [docs/design-ha-modes.md](../design-ha-modes.md))
is tested on one machine with `tests/jepsen/outagefs.c`, a shim that makes the calls of one directory tree hang, fail
or slow down. A shim is not a kernel. These runs use a real NFS server, the way production has it (AlmaLinux 10,
NFSv4.2 over RDMA to ZFS), and answer what the shim cannot: which calls really block in the kernel, for how long, and
whether the process stays alive and answers. Send back the log directory of each run.

One run takes 1 hour; the long variant 3 hours. Do (a) first, it needs no change to the NFS server.

## What N1 does, so you know what to look for

- `tsdbd --health-domain PREFIX` (repeat it, one per NFS server or filesystem) puts every volume whose directory lies
  under PREFIX into a storage domain. The root (VOLUMES, USERS) never belongs to one.
- A probe thread per domain reads `PROBE` and writes `PROBE.<node>` in PREFIX with `O_DIRECT`. The domain goes
  UP, DEGRADED (a probe is late), DOWN (no answer for 10 s) and RECOVERING (answering again, volumes being checked).
  tsdbd logs every change as `tsdbd: storage domain PREFIX: OLD -> NEW` (0 UP, 1 DEGRADED, 2 DOWN, 3 RECOVERING).
- A volume of a DOWN domain answers `TSDB_ERR_UNAVAILABLE` at once, with at most 4 requests inside it; the other
  volumes go on at their normal speed.
- A write or sync error on a volume of a domain no longer ends tsdbd (that is still so for the root and for volumes
  outside a domain): the volume is stopped, waits until every call inside it has returned, is reopened from what is on
  disk (WAL replay, never a retried fsync), checked against what the engine remembers, and resumed. A volume whose
  files lost something that was acknowledged stays stopped (DIVERGED) and tsdbd logs what is missing.

Not in N1 (do not look for it): the mountinfo discovery of domains, parking of requests with a deadline and BUSY, a
bounded per-volume queue, SIGTERM while a call is stuck, the DIVERGED operator command, the high-water cache, the
metrics. A SIGTERM to a tsdbd with a stuck call can hang: stop it with SIGKILL and note how long it took. A tsdbd that
is *started* while a domain is down (a restart during the outage, a systemd restart after the stop timeout) serves no
volume until the heal: tsdb_open waits on the hung directory. Run (d) once with a restart in the middle and note the
time to the first answer after the heal. A DIVERGED volume is accepted by restarting tsdbd; it does not resume by itself.

## Set up

```sh
make                                   # build/tsdbd, build/tsdb, build/outage-load
export NFS=/mnt/tsdb-test              # a test mount of the server under test, NFSv4.2, hard, like production
R=/var/tmp/outage-root                 # the root, on LOCAL disk: it holds VOLUMES and USERS
mkdir -p $NFS/a $NFS/b ~/nfs-outage-report   # for runs (e) and (g), a and b must be on two datasets of the server

build/tsdb $R create probe             # makes the root (any series name will do)
build/tsdb $R volume add $NFS/a
build/tsdb $R volume add $NFS/b
printf 'secret\n' > /var/tmp/outage.pw
build/tsdb $R user add root --superuser --password-file /var/tmp/outage.pw

build/tsdbd --dir $R --port 0 --port-file /var/tmp/outage.port --exit-with-parent=$$ \
    --health-domain $NFS/a --health-domain $NFS/b > ~/nfs-outage-report/tsdbd.log 2>&1 &
echo $! > /var/tmp/outage.pid          # stop it later with kill $(cat /var/tmp/outage.pid), never pkill
sleep 2; read PORT TPID < /var/tmp/outage.port     # the port tsdbd chose, and its pid
```

`--exit-with-parent=$$` stops tsdbd when this shell exits: keep the shell open for the whole run (use `tmux`).

One writer per volume: a row every 10 ms into `m_a` (in `$NFS/a`) and `m_b` (in `$NFS/b`). The ledger records every
call and its answer (acknowledged, refused with UNAVAILABLE, indeterminate):

```sh
build/outage-load run --port $PORT --user root --password-file /var/tmp/outage.pw \
    --series m_a:$NFS/a --series m_b:$NFS/b --ledger ~/nfs-outage-report/ledger --seconds 4000 \
    > ~/nfs-outage-report/load.log 2>&1 &
```

It prints every 10 s, per series, the rows acknowledged, refused and indeterminate so far and the slowest answer in
ms. During an outage `refused` of the sick volume grows and `worst` stays below about 2500 ms; the healthy
volume keeps its rate.

Every 5 seconds, in a second terminal, record what each side does (this is the evidence of every run):

```sh
while sleep 5; do
  date +%T
  for t in $(ls /proc/$(cat /var/tmp/outage.pid)/task); do
    echo "$t $(cat /proc/$(cat /var/tmp/outage.pid)/task/$t/comm) $(cat /proc/$(cat /var/tmp/outage.pid)/task/$t/wchan 2>/dev/null)"
  done | awk '{c[$3]++} END {for (k in c) print "  wchan", k, c[k]}'
  grep -E 'Dirty|Writeback:' /proc/meminfo
done > ~/nfs-outage-report/threads.log
```

(The `wchan` of a thread stuck in `nfs_commit_inode` while it should be doing something else is exactly the finding
that matters for fact 5 of ha.md 15.1. Do not call `df` or `sync` on the NFS mounts from a script that matters: they
hang with the server. Do them once, by hand, in a separate terminal with `timeout 30`, and write down whether they
hung.)

## What every run records

For each run write down, in `~/nfs-outage-report/NAME.md`:

1. The time the server was taken away (`date +%T.%N`) and the time of the first `storage domain ...: 0 -> 1` and
   `-> 2` lines in `tsdbd.log` (time to UNAVAILABLE).
2. During the outage, every 10 s: the rows per second of the healthy volume's clients (must stay above 80% of before)
   and what a client of the sick volume gets (`UNAVAILABLE` within 2.5 s of its request, not a hang).
3. `timeout 30 df $NFS/a; echo rc=$?` and `timeout 30 sync -f $NFS/a; echo rc=$?`: did they hang?
4. Whether tsdbd stayed alive (`kill -0 $(cat /var/tmp/outage.pid)`), whether it answered on the healthy volume,
   and the threads in D state (`ps -L -o stat,wchan:30,comm -p PID | grep '^D'`).
5. The time the server came back and the time of the `-> 3` (RECOVERING) and `-> 0` (UP) lines (time to recovery),
   and the `volume N: ...` lines of the recovery (rows relogged, parts lost).
6. After recovery, the check, which reads both series back and compares them with the ledger (an acknowledged row
   must be there, a refused one must not):

   ```sh
   build/outage-load check --port $PORT --user root --password-file /var/tmp/outage.pw \
       --ledger ~/nfs-outage-report/ledger | tee ~/nfs-outage-report/NAME-check.txt
   ```

   It must end with `ok`. Run it at the end of every run, and again after (d) and (e), with the ledger of the run.

## Runs

Run each for 1 hour with the outage in the middle (10 minutes of load, outage 10 minutes, load again 40 minutes).
Do (a) to (g) in this order. The long variant of (a) and (d) is 3 hours with a 60 minute outage.

**(a) Packets dropped, a TCP test mount only.** On the client, so the server and its other clients are not touched:

```sh
sudo mount -t nfs4 -o vers=4.2,proto=tcp,hard,timeo=100,retrans=2,lookupcache=positive SERVER:/export/test $NFS
SERVERIP=$(getent hosts SERVER | awk '{print $1}')
sudo nft add table inet outage
sudo nft add chain inet outage out '{ type filter hook output priority 0; }'
sudo nft add rule inet outage out ip daddr $SERVERIP tcp dport 2049 drop     # the outage starts
sudo nft delete table inet outage                                            # the outage ends
```

Expect: DOWN after about 10 s, UNAVAILABLE for volumes under `$NFS`, nothing else changes. After the delete the TCP
connection recovers by itself (minutes at the worst, with the retransmit backoff): note the time.

**(b) Link down on the NFS server (RDMA).** `sudo ip link set IFACE down` on the server for 10 minutes, then `up`.
Note the time the client needs to reconnect: the RDMA backoff is 5 s growing to 30 s.

**(c) nfsd threads set to 0.** On the server: `echo 0 | sudo tee /proc/fs/nfsd/threads` for 10 minutes, then back to
the old number (`cat` it first). Clients see no answers, but the TCP or RDMA connection stays up.

**(d) Server reboot** with `grace-time` 45, then 30, then 10 (`/etc/nfs.conf`, `[nfsd] grace-time=`). Reboot the
server and record: the times of DOWN and UP, the number of volumes that reached UP without a reopen error, and how
early grace ended (`/proc/fs/nfsd/...` or the server log). With 10 expect some volumes to log
`reopen failed, will retry`: a volume whose locks were lost reopens with an error until grace has passed. Note the
count in each run.

**(e) `zpool export` of one pool while another stays up.** `$NFS/a` on pool one, `$NFS/b` on pool two.
`sudo zpool export POOL1` (use `-f` if it is busy), 10 minutes, `sudo zpool import POOL1`. Expect: only the domain of
POOL1 goes DOWN (give each `--health-domain` its own dataset path for this to show), the other keeps its rate.
After the import tsdbd must recover without a restart. Also try once with `rollback`: `zfs rollback` a snapshot taken
before the run on the dataset behind `$NFS/a` while tsdbd is stopped, then start it again: expect a refusal to open
the volume or DIVERGED with the lost parts named (the part payloads are not compared, only the files and their sizes;
damaged payloads are found only with `verify_checksums`, so start this run with `--verify-checksums` if you have it).

**(f) A dataset quota filled to ENOSPC.** `sudo zfs set quota=SIZE POOL/a` a little above the current use, let the
load fill it. Expect: the volume fails with `no space` (not UNAVAILABLE for 10 minutes if the probe still works),
tsdbd does not exit, the other volume goes on. Raise the quota: the volume must come back through the in-place
recovery, with no acknowledged row lost.

**(g) A dataset unmounted under its export.** `sudo zfs unmount POOL/a` on the server, so that the export of `a`
now shows the empty directory of the parent dataset. Expect (with `mp` in the export options) the server refuses the
export and the client gets errors, not an empty volume. Without `mp`, write down what tsdbd sees: the volume has
no files. This is the case "mount identity" in ha.md 15.2; N1 does not check it, so record whether tsdbd opens an
empty database there. Note this finding first in the report.

## Send back

`~/nfs-outage-report/` with, per run: the NAME.md notes, `tsdbd.log`, `threads.log`, `load.log`, the kernel log of the
client (`dmesg -T | grep -i -E 'nfs|rdma|hung|blocked'`) and the output of `nfsstat -c` before and after.
