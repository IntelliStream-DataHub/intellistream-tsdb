# NFS servers for tsdb

This guide sets up the NFS servers that hold tsdb's data, and the tsdb nodes
that mount them. It is written for **AlmaLinux 10** on both sides, with ZFS on
the servers and NFSv4.2 over RDMA (fast network transfers that bypass most of
the CPU). [ha.md](ha.md) explains the design behind each setting; this guide
says what to type.

**How sure we are.** The facts here were checked against the Linux 6.12 kernel
sources, nfs-utils 2.8.3, the CentOS Stream 10 kernel config and nfs-utils
package, the RHEL 10 guide "Configuring and using network file services", and
the OpenZFS documentation. AlmaLinux 10 is rebuilt from the same sources as
CentOS Stream 10 and RHEL 10, but **we did not check AlmaLinux 10 itself**.
Anything we could not check is marked **UNVERIFIED**. The checklist at the end
tells you how to confirm each point on your own machines.

## Contents

1. [What tsdb needs from the NFS server, and why](#1-what-tsdb-needs-from-the-nfs-server-and-why)
2. [Setting up an NFS server](#2-setting-up-an-nfs-server)
3. [Setting up the tsdb nodes](#3-setting-up-the-tsdb-nodes)
4. [Verification checklist](#4-verification-checklist)
5. [What happens when an NFS server restarts](#5-what-happens-when-an-nfs-server-restarts)
6. [Troubleshooting](#6-troubleshooting)

## 1. What tsdb needs from the NFS server, and why

tsdb writes four kinds of files: part files (written once, MBs to GBs, then
never changed), a write-ahead log or WAL (small records appended and flushed to
disk on every commit), a small MANIFEST rewritten in place, and small
dictionaries that only grow. Each volume has exactly one writer node at a
time. The NFS server decides which node that is.

**Leases (10 s).** A lease is the server's promise to keep a client's open
files and locks while the client keeps checking in. tsdb's writer holds a lock
on each volume's `LOCK` file. If the writer node dies, the server lets the lock
go once the lease runs out, and another node takes the volume over. A short
lease means a fast failover: about 11.5 s with `lease-time=10`. 10 s is the
lowest value the kernel accepts.

**Grace (10 s, ended early by nfsdcld).** After an NFS server restarts, it
gives its clients a *grace period* to take back ("reclaim") the files and locks
they held. During grace nobody may take a new lock, so no other node can steal
a volume from its writer. tsdb wants grace short, because every new file open
on that server waits until it ends. **nfsdcld** (the NFSv4 client tracking
daemon) remembers on disk which clients the server had. With it, the server
ends grace as soon as all of those clients have reclaimed, usually within
seconds. **Without nfsdcld, the AlmaLinux 10 kernel refuses every reclaim**
(section 2.4 explains why), so every restart costs every node its locks.
nfsdcld is required, not optional.

**Delegations off.** A delegation lets a client cache a file and act on it
without asking the server. tsdb's safety depends on every lock and every write
going through the server, so delegations must be off
(`fs.leases-enable = 0`).

**One export per pool.** An export is a directory the server shares over NFS.
tsdb tracks the health of each export separately. If one ZFS pool fails, only
the volumes in that pool stop. One export per pool keeps "export healthy" and
"pool healthy" the same thing. The `mp` export option makes sure the server
never shares the empty directory underneath a pool that failed to mount.

**Sync durability.** When tsdb says a write is saved, it must be on disk on the
server. That needs `sync` exports and ZFS `sync=standard`. A mirrored SLOG (a
fast separate log device) makes the WAL's small flushes fast without giving up
safety.

## 2. Setting up an NFS server

Do these steps on each NFS server, in this order. Commands run as root. In
the examples, the server has one pool called `pool1`, the tsdb nodes are on
`10.0.0.0/24`, and the RDMA network is already configured.

### 2.1 Packages

```sh
dnf install nfs-utils rdma-core
```

nfs-utils contains the NFS server, `nfsdctl`, and nfsdcld. There is no
separate nfsdcld package. ZFS comes from the OpenZFS repository for EL10;
install it as you do today.

### 2.2 ZFS: pool and dataset

Give tsdb one dataset per pool, and no child datasets under it.

**The pool.** Use a **mirrored SLOG with power-loss protection**. If a single
log device dies together with a crash, the last seconds of saved writes are
lost. About 16-32 GB of log is plenty; ZFS gains nothing from a log larger
than its RAM cache. Over-provision the rest of a flash device.

```sh
# Add a mirrored log to an existing pool (device names are examples)
zpool add pool1 log mirror /dev/disk/by-id/nvme-SLOG-A /dev/disk/by-id/nvme-SLOG-B
```

Keep the pool's `failmode` at its default, `wait`: writes to a suspended pool
then hang, which is the case tsdb's own probes are designed to detect
([ha.md](ha.md) §15.2). **UNVERIFIED:** how tsdb behaves with
`failmode=continue` (new writes fail with EIO); this was not checked.

**The dataset.**

```sh
zfs create \
  -o sync=standard \
  -o logbias=latency \
  -o recordsize=1M \
  -o compression=lz4 \
  -o atime=off \
  -o xattr=sa \
  -o dnodesize=auto \
  -o redundant_metadata=all \
  -o sharenfs=off \
  pool1/tsdb
```

Why each setting:

| Setting | Why |
|---|---|
| `sync=standard` | **Required.** `disabled` makes the server confirm writes that are not on disk, which breaks tsdb's durability promise. `always` is very slow. |
| `logbias=latency` | The default. Small flushes (the WAL) go to the fast SLOG. |
| `recordsize=1M` | Part files are written once, in order; large records suit them. Applies only to files created after you set it. If benchmarks show the WAL suffers, try `128K` (measure with `build/insert-bench`). |
| `compression=lz4` | Costs almost nothing. Part files are already compressed, but the MANIFEST, the dictionaries and partly filled records shrink. Do not use zstd or gzip here. |
| `atime=off` | tsdb never reads access times; this saves a write on every read. |
| `xattr=sa`, `dnodesize=auto` | Stores extended attributes (SELinux labels, on by default in EL10) inside the file's metadata instead of in extra blocks. |
| `redundant_metadata=all` | The default. `most` gains only a few percent. |
| `sharenfs=off` | `/etc/exports` alone manages the exports, not ZFS. |

`dnodesize=auto` needs the pool feature `large_dnode` (`zpool get
feature@large_dnode pool1` must say `enabled` or `active`); on an old pool,
`zfs create` fails with this option until you run `zpool upgrade pool1`.

To change an existing dataset, use `zfs set`, for example
`zfs set atime=off pool1/tsdb`.

### 2.3 /etc/nfs.conf

Edit `/etc/nfs.conf` and set these keys. Keep the rest of the file as it is.

```ini
[nfsd]
# A lease is how long the server keeps a silent client's locks. 10 is the minimum.
lease-time=10
# Grace after a restart. nfsdcld ends it early (section 5).
grace-time=10
# NFSv4.0 clients never tell the server they are done reclaiming,
# so one of them would hold every grace period open to its full length.
# Check first that no other client of this server mounts with vers=4.0.
vers4.0=n
vers4.1=y
vers4.2=y
# RDMA listener. Already set in the default file of nfs-utils 2.8.3.
rdma=y
rdma-port=20049

[nfsdcld]
# Where nfsdcld keeps its client database. Must be on the local OS disk.
storagedir=/var/lib/nfs/nfsdcld
```

Notes:
- `lease-time` and `grace-time` accept 10 to 3600 seconds. The kernel default
  is 90 for both.
- The server reads them only when NFS starts. Changing them needs
  `systemctl restart nfs-server` (section 2.8). Writing to the files in
  `/proc/fs/nfsd` while the server runs fails with "Device or resource busy".
- `storagedir` shows the default. Keep it on the local OS disk, never on an
  exported pool: the database must survive a reboot, and nfsdcld starts before
  ZFS datasets are guaranteed to be mounted (`nfsdcld.service` has
  `DefaultDependencies=no` and is ordered only after `systemd-remount-fs`).
  If `/var` or `/var/lib` is a separate filesystem, add a drop-in to
  `nfsdcld.service` with `[Unit]` `RequiresMountsFor=/var/lib/nfs/nfsdcld`
  (`systemctl edit nfsdcld`), or nfsdcld may start on the empty directory
  underneath it.
- The number of server threads (`threads=` in `[nfsd]`) is not covered here.
  Raise it if `nfsstat` shows the threads all busy. Without `threads=`,
  `nfsdctl autostart` in nfs-utils 2.8.3 starts 16 (`DEFAULT_AUTOSTART_THREADS`).
  **UNVERIFIED:** that the AlmaLinux 10 package keeps that default.

### 2.4 nfsdcld: enable and verify

**Why it is required.** The AlmaLinux 10 kernel is built without the two older
ways of tracking clients (CentOS Stream 10 sets
`# CONFIG_NFSD_LEGACY_CLIENT_TRACKING is not set`; the upstream default is
`y`, so check your kernel with
`grep NFSD_LEGACY /boot/config-$(uname -r)`). nfsdcld is the only one
left. If nfsdcld is not running when NFS starts, the kernel logs "NFSD: Unable
to initialize client recovery tracking!" and then refuses *every* reclaim
after a restart, while still making all clients wait out the full grace
period. Every tsdb node then loses its locks on that server.

**Is it already running?** On CentOS Stream 10, `nfs-server.service` already
pulls nfsdcld in (`Wants=nfsdcld.service`, `After=nfsdcld.service`). So it may
well be running on your servers today. Check before changing anything:

```sh
systemctl status nfsdcld
systemctl list-dependencies nfs-server | grep nfsdcld
journalctl -k -b | grep 'NFSD:'
```

If it is in use, the kernel log contains:

```
NFSD: Using nfsdcld client tracking operations.
```

If you see "Unable to initialize client recovery tracking!" or "Is nfsdcld
running?" instead, enable it as follows.

**Enable it.**

1. Make sure no local override hides the unit. `nfsdcld.service` has no
   `[Install]` section, so `systemctl enable nfsdcld` does nothing; it is
   started by `nfs-server.service`.

   ```sh
   systemctl is-enabled nfsdcld        # expect: static
   systemctl unmask nfsdcld            # only if the line above printed "masked"
   systemctl add-wants nfs-server.service nfsdcld.service   # only if list-dependencies did not show it
   ```

2. Create the state directory if you want to be sure of its owner and mode
   (nfsdcld creates it on its own otherwise). The nfsdcld man page says it
   should be owned by root and readable and writable by its owner; 0700 does
   that.

   ```sh
   install -d -o root -g root -m 0700 /var/lib/nfs/nfsdcld
   ```

3. Plan a maintenance window. The kernel picks its client tracking only when
   NFS starts, so enabling nfsdcld needs an NFS restart. That restart starts a
   grace period, and because no client is recorded yet, **every client loses
   its locks once**. Do it before tsdb HA goes live, or stop `tsdbd` on all
   nodes that use this server first.

4. Restart NFS so that nfsdcld starts first:

   ```sh
   systemctl stop nfs-server
   systemctl start nfsdcld
   systemctl start nfs-server
   ```

   The kernel waits only about 1 second for nfsdcld when NFS starts, so
   nfsdcld must already be running.

5. Verify:

   ```sh
   journalctl -k -b | grep 'NFSD:'
   # NFSD: Using nfsdcld client tracking operations.
   # and either "NFSD: starting 10-second grace period ..."
   # or "NFSD: no clients to reclaim, skipping NFSv4 grace period ..."
   ls -l /var/lib/nfs/nfsdcld/main.sqlite
   nfsdclddb print      # lists the clients nfsdcld remembers
   ```

   Once tsdb nodes have mounted, `nfsdclddb print` lists one record per node.

**Old records.** On its first start nfsdcld copies any records left by the
older tracking methods, then clears them. On an AlmaLinux 10 kernel neither
older method could have been active, so there is normally nothing to copy.

**Keep the database.** Do not delete `/var/lib/nfs/nfsdcld`. Losing it has the
same effect as running without nfsdcld for one restart: every client loses its
locks.

### 2.5 RDMA

On the server (from the RHEL 10 guide, section 2.10):

1. `rdma-core` is installed (section 2.1).
2. In `/etc/rdma/modules/rdma.conf`, the lines `xprtrdma` (client) and
   `svcrdma` (server) must not be commented out.
3. The IP interface on the RDMA fabric (IPoIB on InfiniBand) is configured.
4. Open the port:

   ```sh
   firewall-cmd --permanent --add-port={20049/tcp,20049/udp}
   firewall-cmd --reload
   ```

5. `rdma=y` and `rdma-port=20049` are in `/etc/nfs.conf` (section 2.3).

### 2.6 Exports

One line per pool in `/etc/exports`:

```
/pool1/tsdb  10.0.0.0/24(rw,sync,mp,no_subtree_check,sec=sys,fsid=3f1c9e2a-6b0d-4d47-9a51-0c6e8a7d2b10)
```

| Option | Why |
|---|---|
| `rw` | tsdb writes. |
| `sync` | The server confirms a write only once it is on disk. It is the default; write it anyway. |
| `mp` | Export only if the directory is a mount point. If the ZFS dataset failed to mount, the server shares nothing instead of the empty directory under it. |
| `no_subtree_check` | The default. Without it written, `exportfs` warns. |
| `sec=sys` | Plain user ids. On a closed RDMA network this is enough; Kerberos encryption (`krb5p`) would cost CPU on every request. |
| `fsid=<uuid>` | A fixed identity for the export. Clients' file handles contain it. If it ever changed, every client would get "Stale file handle" errors. Generate one per export with `uuidgen`, unique across all servers. Set it before any client mounts; changing it later causes exactly those errors. |

Do **not** use `crossmnt`, and do not create child datasets under the
exported one. With `crossmnt`, a child dataset that fails to mount shows its
empty directory, and `mp` does not catch that.

Apply and check:

```sh
exportfs -ra
exportfs -v
```

`/etc/exports` is read when NFS starts and on `exportfs -r`. The `mp` check
itself is made by `rpc.mountd` each time the kernel asks about the export
(nfs-utils `support/export/cache.c`): while the dataset is not mounted, mountd
gives no answer and clients wait (hard mounts) instead of seeing the empty
directory, and once it is mounted the export works without another
`exportfs`. If clients keep waiting after the pool is mounted, run
`exportfs -f` (flush the kernel's export cache) or `exportfs -ra`.
**UNVERIFIED:** how long the kernel takes to ask again on its own.

### 2.7 sysctl: delegations off

```sh
echo 'fs.leases-enable = 0' > /etc/sysctl.d/90-nfsd-nodeleg.conf
sysctl --system
```

This also turns off file leases for local programs on the server (for example
Samba oplocks). Keep these servers for NFS only.

The sysctl is checked in the kernel's `generic_setlease`, which nfsd reaches
only if the filesystem has no `setlease` method of its own (`kernel_setlease`,
`fs/locks.c`). **UNVERIFIED:** that OpenZFS defines none. The "No delegations
held" check in section 4 confirms the result either way.

### 2.8 Start order

At boot, and when you restart by hand, things must come up in this order:

1. The ZFS pools are imported and `pool1/tsdb` is mounted.
2. The sysctl from section 2.7 is applied (systemd does this early at boot).
3. nfsdcld is running.
4. `nfs-server` starts. It reads `/etc/nfs.conf`, loads the exports and starts
   the kernel NFS server.

Steps 3 and 4 are already ordered by the units. For step 1, add an ordering so
that NFS starts after ZFS has mounted its datasets. This is a precaution: `mp`
already keeps an unmounted dataset from being shared, but without the ordering
the export would be missing until you run `exportfs -ra`.

```sh
mkdir -p /etc/systemd/system/nfs-server.service.d
cat > /etc/systemd/system/nfs-server.service.d/after-zfs.conf <<'EOF'
[Unit]
After=zfs-mount.service
Wants=zfs-mount.service
EOF
systemctl daemon-reload
```

**UNVERIFIED:** whether the OpenZFS EL10 packages already order these units.
The drop-in is harmless if they do.

After any change to `/etc/nfs.conf` or the sysctl:

```sh
systemctl restart nfs-server
```

This starts a grace period (section 5).

## 3. Setting up the tsdb nodes

### 3.1 Packages

```sh
dnf install nfs-utils rdma-core
```

Check `/etc/rdma/modules/rdma.conf` as on the server: `xprtrdma` must not be
commented out.

### 3.2 A unique client identity

The server knows each client by a name. By default that name is built from
the hostname. Two nodes with the same name (a cloned image, a reused
hostname) look to the server like one client that rebooted: each time one of
them connects, the server throws away the other's open files and locks. That
breaks tsdb's single-writer guarantee. So give every node its own id.

Generate a fresh id **on each node** (do not derive it from `/etc/machine-id`,
which cloned images share):

```sh
id=$(uuidgen)
echo "options nfs nfs4_unique_id=$id recover_lost_locks=0" > /etc/modprobe.d/nfs.conf
```

- The parameter belongs to the `nfs` module, so its full name is
  `nfs.nfs4_unique_id`. The nfs(5) man page writes `nfs4.nfs4_unique_id`; that
  name is wrong.
- `recover_lost_locks=0`: after a lock is lost, the client must not quietly
  take it again. tsdb refuses to open a volume otherwise.
- The setting applies only before the first NFSv4 mount. Reboot the node (or
  unmount every NFS mount and reload the `nfs` module).
- You can also put `nfs.nfs4_unique_id=<uuid>` on the kernel command line.

**UNVERIFIED:** whether any EL10 package sets this id by default. Nothing in
nfs-utils does.

### 3.3 Mount options

```
vers=4.2,proto=rdma,port=20049,hard,timeo=100,retrans=2,lookupcache=positive
```

| Option | Why |
|---|---|
| `vers=4.2` | NFSv4.1 or later is needed so the client tells the server when it is done reclaiming; 4.2 is the newest. |
| `proto=rdma` | Use RDMA instead of TCP. |
| `port=20049` | The standard NFS-over-RDMA port. |
| `hard` | Never fail a request just because the server is slow. tsdb detects outages itself, and a "soft" timeout would leave a write's outcome unknown. |
| `timeo=100` | Wait 10 s (in tenths of a second) before retrying a request. |
| `retrans=2` | After 2 retries, report "server not responding" and reconnect. On a hard mount these two only control that, never an error. |
| `lookupcache=positive` | Do not remember that a file was missing, so a file that another node just created is seen at once. |
| no `local_lock` | Locks must go to the server: they are how the server picks the writer. Do not add `local_lock=...`; for NFSv4 the default is `none`. |
| no `nolock`, no `soft` | tsdb refuses to open a volume on such a mount. |

Do not use autofs for tsdb's mounts: tsdb never stats a path, so it would
never trigger one, and its preflight refuses autofs mountpoints
([ha.md](ha.md) §15.2).

**With /etc/fstab:**

```
nfs1:/pool1/tsdb  /tsdb/nfs1-pool1  nfs  vers=4.2,proto=rdma,port=20049,hard,timeo=100,retrans=2,lookupcache=positive,_netdev,nofail  0 0
```

- `_netdev`: mount only once the network is up.
- `nofail`: a server that is down at boot does not stop the node from
  booting. tsdb notices the missing mount and reports those volumes as
  unavailable; it never writes into the empty directory.

**Or as a systemd mount unit.** The file name must match the path:
`/tsdb/nfs1-pool1` becomes `tsdb-nfs1\x2dpool1.mount` (`systemd-escape -p
--suffix=mount /tsdb/nfs1-pool1` prints it). To keep the example simple, use a
path without dashes:

```ini
# /etc/systemd/system/tsdb-nfs1pool1.mount
[Unit]
Description=tsdb data on nfs1:/pool1/tsdb
After=network-online.target
Wants=network-online.target

[Mount]
What=nfs1:/pool1/tsdb
Where=/tsdb/nfs1pool1
Type=nfs
Options=vers=4.2,proto=rdma,port=20049,hard,timeo=100,retrans=2,lookupcache=positive

[Install]
WantedBy=remote-fs.target
```

```sh
mkdir -p /tsdb/nfs1pool1
systemctl daemon-reload
systemctl enable --now tsdb-nfs1pool1.mount
```

## 4. Verification checklist

Run these after setup and after every server or node upgrade.

**On each NFS server:**

| Check | Command | Expected |
|---|---|---|
| nfsdcld runs | `systemctl is-active nfsdcld` | `active` |
| nfsd uses it | `journalctl -k -b \| grep 'NFSD:'` | `Using nfsdcld client tracking operations.` and no "Unable to initialize client recovery tracking" |
| Client records | `nfsdclddb print` | One record per tsdb node (after they mounted) |
| Lease and grace | `cat /proc/fs/nfsd/nfsv4leasetime /proc/fs/nfsd/nfsv4gracetime` | `10` and `10` |
| Grace over | `cat /proc/fs/nfsd/v4_end_grace` | `Y` (once grace has ended) |
| No legacy tracking | `grep NFSD_LEGACY /boot/config-$(uname -r)` | `# CONFIG_NFSD_LEGACY_CLIENT_TRACKING is not set` (then nfsdcld is the only way) |
| NFSv4.0 off | `cat /proc/fs/nfsd/versions` | `-4.0` in the list, for example `-2 +3 +4 -4.0 +4.1 +4.2` (the kernel prints `-4.0` only when 4.0 is off) |
| RDMA listener | `cat /proc/fs/nfsd/portlist` | a line `rdma 20049` |
| RDMA modules | `lsmod \| grep -E 'svcrdma\|rpcrdma'` | at least one line |
| Exports | `exportfs -v` | each pool with `sync`, `mountpoint` (that is how `exportfs -v` prints `mp`), `no_subtree_check`, `fsid=...`; no `crossmnt` |
| Delegations off | `sysctl fs.leases-enable` | `fs.leases-enable = 0` |
| No delegations held | `grep -l deleg /proc/fs/nfsd/clients/*/states` | no output |
| Client names unique | `grep -h '^name:' /proc/fs/nfsd/clients/*/info \| sort \| uniq -d` | no output (the `^` matters: every info file also has an `Implementation name:` line, the same on all nodes with one kernel) |
| ZFS settings | `zfs get sync,logbias,recordsize,compression,atime,xattr,dnodesize,redundant_metadata,sharenfs pool1/tsdb` | the values from section 2.2 |
| Mirrored SLOG | `zpool status pool1` | a `logs` section with a `mirror` |
| SLOG in use | `zpool iostat -v pool1 1` while tsdb writes | writes on the log devices |

**On each tsdb node:**

| Check | Command | Expected |
|---|---|---|
| Mount options | `nfsstat -m` | `vers=4.2`, `proto=rdma`, `port=20049`, `hard`, `timeo=100`, `retrans=2`, `lookupcache=pos`, `local_lock=none` |
| Client id | `cat /sys/module/nfs/parameters/nfs4_unique_id` | the uuid you set, different on every node |
| Per-namespace id | `cat /sys/fs/nfs/net/nfs_client/identifier` | empty, or a value unique to this node (it overrides the module setting) |
| Lost locks | `cat /sys/module/nfs/parameters/recover_lost_locks` | `N` |

## 5. What happens when an NFS server restarts

With the settings above, a planned or unplanned restart goes like this.

1. **The server goes away.** tsdb nodes' requests to it wait (hard mounts).
   After `timeo` and `retrans` run out the kernel logs "server not
   responding" and drops the RDMA connection (**UNVERIFIED:** how long that
   takes with `timeo=100,retrans=2`; assumption U30 in [ha.md](ha.md)). tsdb notices much sooner with its
   own probes, marks the volumes on that server unavailable, and answers
   clients at once. Pulsar keeps their writes until the volumes return.
   Volumes on other servers keep running at full speed.
2. **The server comes back and NFS starts.** nfsdcld has the list of clients
   from before the restart. The kernel logs "starting 10-second grace period".
   During grace, clients may only reclaim. Every new file open and every new
   lock, from every client, waits.
3. **The nodes reconnect at once.** A node's own reconnect timer can wait up to
   30 s, far longer than grace. tsdbd therefore forces the RDMA connection to
   reconnect as soon as its probe sees the server answer again.
   **UNVERIFIED:** that this forced reconnect works (assumption U41 in
   [ha.md](ha.md), tested before grace is lowered to 10 s in production; the
   fallback is `grace-time=45`).
4. **Each node reclaims** its open files and locks, then tells the server it is
   done.
5. **Grace ends early.** When every client that nfsdcld remembered has said it
   is done, the kernel logs "all clients done reclaiming, ending NFSv4 grace
   period". Usually this is a few seconds after NFS started.
   - A remembered node that is down keeps grace open for the full 10 s.
   - While nodes are still reclaiming, the kernel extends grace past 10 s,
     checking about once a second whether anyone reclaimed since the last
     check. The code means to stop this at twice the lease (20 s after NFS
     started), but in the 6.12 source that cap never fires: it compares the
     time since boot with a wall-clock start time (`clients_still_reclaiming`
     and `nfs4_state_create_net` in `fs/nfsd/nfs4state.c`). So grace ends only
     once reclaims stop. **UNVERIFIED:** whether the AlmaLinux 10 kernel fixes
     this; measure it.
   - If nfsdcld remembers no clients, there is no grace at all ("no clients to
     reclaim, skipping NFSv4 grace period").
6. **tsdb checks the storage** before it writes again: the WAL, the MANIFEST
   and the parts must match what it confirmed to clients before the outage.
   Then the volumes become available.

**A node that misses grace** (it reconnects too late) loses its locks on that
server. With HA, its volumes there move to other nodes; without HA, the
files holding its locks return EIO, and tsdb takes the re-log path
([ha.md](ha.md) §15.1, §15.5). In both cases no write that tsdb
confirmed is lost, because every commit is checked against the lock.

**Without nfsdcld** step 5 never comes early and every reclaim in step 4 is
refused. Every node waits the full grace period and then loses all its locks
on that server, so all its volumes there fail over at once.

## 6. Troubleshooting

**Kernel log: "NFSD: Unable to initialize client recovery tracking!" / "Is
nfsdcld running?"** nfsdcld was not running when NFS started. Do steps 1 and 4
of section 2.4. Then check that `/var/lib/nfs/nfsdcld` is on a local disk that
is mounted at boot.

**Grace never ends early.** Look for a client that never says it is done:
- an NFSv4.0 client: check `vers4.0=n` and the client's mount (`vers=4.2`);
- a remembered node that is down or was retired: `nfsdclddb print` lists the
  names nfsdcld expects. A record is removed when the server expires the
  client (`expire_client` calls `nfsd4_client_record_remove`). A client whose
  lease ran out but that still holds opens or locks is kept as a "courtesy"
  client first, and only expired when another client conflicts with its state
  or after a long timeout. **UNVERIFIED:** that timeout on 6.12 (the constant
  was not read), so a retired node can keep holding grace open for a while.
  Unmount it cleanly before retiring it.

**`nfsv4leasetime` or `nfsv4gracetime` shows 90.** The values in
`/etc/nfs.conf` were not read: check that they are under `[nfsd]` and not
commented out, then `systemctl restart nfs-server`.

**"Device or resource busy" when writing to `/proc/fs/nfsd/...`** NFS is
running. Set the value in `/etc/nfs.conf` and restart NFS instead.

**No `rdma 20049` in `/proc/fs/nfsd/portlist`.** Check `rdma=y` in
`[nfsd]`, that `svcrdma` is loaded (`modprobe svcrdma`), and the RDMA
interface. Restart NFS.

**Mount fails or uses TCP.** Check `proto=rdma,port=20049` in the options,
that `xprtrdma` is loaded on the node, and the firewall on the server.

**Two nodes keep losing their locks, or the server log shows clients
replacing each other.** Their client names collide. Run the duplicate check
from section 4 on the server, and give each node its own
`nfs4_unique_id` (section 3.2).

**"Stale file handle" (ESTALE) on every file after a pool was recreated or
re-imported.** The export's identity changed. Set a fixed `fsid=<uuid>` on the
export (section 2.6), then remount the clients.
**UNVERIFIED:** whether ZFS keeps the identity stable across a re-import
without an explicit `fsid`.

**An export is missing after a reboot.** The pool was not mounted when
clients asked, and `mp` correctly hid the empty directory. Once the dataset is
mounted the export should come back by itself (section 2.6); if not, run
`exportfs -f` or `exportfs -ra`. Add the start ordering from section 2.8.

**A node hangs at boot waiting for a mount.** Add `nofail` to the fstab line
(section 3.3).

**`/proc/fs/nfsd/clients/*/states` still shows `deleg`.** The sysctl was
applied after those delegations were granted. Restart NFS.
**UNVERIFIED:** whether existing delegations stay until clients return them.

**`zfs get sync` shows `disabled`.** Set it back at once
(`zfs set sync=standard pool1/tsdb`). Writes confirmed while it was disabled
may not have been on disk.
