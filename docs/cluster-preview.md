# The cluster preview: a tutorial

This page sets up a small tsdb cluster on one machine, looks at it, breaks it on purpose and puts it back together.
It takes about ten minutes. Read the first section before you start: it says what the preview is, and what it is not.

## What this is, and what it is not

Release 0.10.0 has the first piece of tsdb's cluster: the **metadata group**. That is a small group of servers that
agree, with the Raft protocol, on a list of facts about the cluster:

- which servers are **members**, where they are, and whether they are up or down;
- which servers are **voters** (they decide) and which only **watch**;
- the settings of the cluster (how many copies, how long a server may stay quiet);
- the places where **volumes**, **owners**, **epochs**, **in-sync sets**, **users** and **grants** will be kept.

A **witness** is a member with no data. It only keeps the list and can vote. It is a cheap third or fifth vote.

What the preview does **not** do yet. Please do not skip this list:

- **It does not copy your data.** A row you write to one server stays on that server. Step 6 shows it.
- **It does not fail over.** If a server dies, nothing takes over its data. The group notices (it marks the server
  down) and that is all.
- **It does not move volumes**, and you cannot add a volume or a user to the cluster yet.
- **It changes nothing for clients.** The client port, the protocol and every request work exactly as without
  `--cluster`. A client talks to one server, as before. Logins are still per server (`tsdb DIR user ...`).
- **It is not for production.** The format of the metadata and the peer protocol may change in a later 0.x
  release. A build of the preview does not talk to a build from before such a change: you start the cluster again
  with `tsdb cluster init`, you do not upgrade it.

Why ship it then? The group is the part everything else stands on, and it is the part that is hardest to get right.
It has its own simulator, a checker and fault tests, and now you can run it yourself. What comes next is at the
[end of this page](#whats-next).

If you do not use `--cluster`, none of this runs, and nothing about your server changes.

## What you need

- A checkout of tsdb, built with `make` (the first step below does it). Linux. No root.
- Four free ports for the servers' client side (17081 to 17084) and four for their peer side (17091 to 17094), on
  `127.0.0.1`. If something else uses one of them, change the numbers in the two lines marked `# ports` in step 3.
- Nothing else. The block only writes under `/tmp/tsdb-cluster`.

## The tutorial

Paste the whole block into a terminal, in the top directory of the checkout. It stops at the first command that
fails. Our tests run this block word for word (`tests/test_cluster_tutorial.sh`), so it works as written.
The text after the block explains each step and shows what you should see.

<!-- cluster-tutorial: tests/test_cluster_tutorial.sh runs this block word for word -->
```sh
# 1. Build. Everything lands in build/.
make

# 2. A scratch directory, and the cluster's secret. The secret is a file with 32 random bytes written as hex
#    (mode 0600). Every server and every `tsdb cluster` command reads it. Whoever has it can change the cluster.
D=/tmp/tsdb-cluster
rm -rf $D && mkdir -p $D
build/tsdb cluster secret $D/secret

# 3. Helpers. A "node" is a server with its own data directory (n1/data) and its own state directory (n1/state).
#    The state directory holds the cluster's own files and must be on a local disk, never on NFS.
#    Node N listens for clients on port 1708N and for the other servers on port 1709N.
#    --no-auth: no login, because users are still per server in the preview. Fine on 127.0.0.1, nowhere else.
#    A witness (node 4) has no data directory and no client port.
peer() { echo 127.0.0.1:1709$1; }          # ports: the peer address of node N
port() { echo 1708$1; }                    # ports: the client port of node N
start_node() {                             # start_node N [witness]: start a server, remember its process id
    mkdir -p $D/n$1/state
    if [ "${2:-}" = witness ]; then role="--witness --advertise 127.0.0.1"
    else mkdir -p $D/n$1/data; role="--dir $D/n$1/data --no-auth --port 1708$1"; fi
    build/tsdbd $role --state-dir $D/n$1/state --peer-port 1709$1 \
        --cluster --cluster-secret-file $D/secret --rack rack$1 >> $D/n$1/log 2>&1 &
    echo $! > $D/n$1/pid
}
status() { build/tsdb cluster status $(peer $1) --secret-file $D/secret; }    # status N: ask node N
wait_for() {                               # wait_for N REGEX: wait (30 s at most) until node N's status has such a line
    for i in $(seq 150); do status $1 2>/dev/null | grep -q "$2" && return 0; sleep 0.2; done
    echo "gave up waiting for: $2"; return 1
}

# 4. Start three servers. They know nothing of each other yet: each one says it is in no cluster.
start_node 1; start_node 2; start_node 3
for n in 1 2 3; do wait_for $n '^node'; done
status 1

# 5. Make the cluster on node 1 (it becomes node 1 and the leader), then add the other two through it.
#    The group grows by itself: one voter, then three. Wait until all three vote, then look at it from node 2.
build/tsdb cluster init $(peer 1) --secret-file $D/secret
build/tsdb cluster node add $(peer 1) $(peer 2) --secret-file $D/secret
build/tsdb cluster node add $(peer 1) $(peer 3) --secret-file $D/secret
wait_for 2 '^group voters 1,2,3 '
status 2

# 6. Nothing changed for clients, and nothing is copied. Write a day of readings to node 1 as if it were
#    a lone server, then list the series in node 1's directory and in node 2's.
build/client-example 127.0.0.1 $(port 1)
build/tsdb $D/n1/data series | cut -f1,2
echo "node 2 has $(build/tsdb $D/n2/data series | wc -l) series"

# 7. Kill the leader, as a crash would (kill -9, by its process id). The others notice after three seconds without
#    its heartbeat, elect a new leader, and mark the dead node down. Ask a survivor.
leader=$(status 2 | awk 'NR == 1 { print $10 }')
other=$(( leader % 3 + 1 ))
echo "node $leader is the leader: stopping it"
kill -9 $(cat $D/n$leader/pid)
wait_for $other "^member $leader down"
status $other

# 8. Start it again on the same directories. It comes back as the same member with a new incarnation (a counter of
#    its starts), and it is up again.
start_node $leader
wait_for $leader "^member $leader up incarnation 2"
status $leader

# 9. A witness: a member without data. Start it, add it through any member, look at it. With three voters it only
#    watches (the group keeps an odd number of voters); it takes a voter's place when one is lost for good.
start_node 4 witness
wait_for 4 '^node'
build/tsdb cluster node add $(peer 1) $(peer 4) --secret-file $D/secret
wait_for 4 '^member 4 '
status 4

# 10. What the servers said about the leaders and the members' state, from their logs (the details differ on your machine).
grep -h 'leads the metadata group\|proposes node' $D/n[123]/log

# 11. Stop every server by its process id, wait until they are gone, and clean up.
for n in 1 2 3 4; do kill $(cat $D/n$n/pid); done
for n in 1 2 3 4; do while kill -0 $(cat $D/n$n/pid) 2>/dev/null; do sleep 0.1; done; done
rm -rf $D
```

## What you should see

The lines below come from a real run. Yours differ in the places the test ignores: the cluster's id (a random
number), the times after `heard`, the term and commit numbers, and which node leads. The test compares every
other line of this page with the real output.

Step 2 writes the secret:

<!-- cluster-tutorial-output -->
```
wrote a new cluster secret to /tmp/tsdb-cluster/secret (copy it to every server; tsdbd --cluster-secret-file)
```

Step 4: a server that is in no cluster yet. `mode none` and `no state yet` say so.

<!-- cluster-tutorial-output -->
```
node 0 mode none role - term 0 leader 0 commit 0 applied 0
cluster (no state yet)
```

Step 5: `init` and `node add` answer with one line each.

<!-- cluster-tutorial-output -->
```
initialised: node 1 at 127.0.0.1:17091
added 127.0.0.1:17092
added 127.0.0.1:17093
```

and `status 2` shows the cluster, asked of node 2, which follows:

<!-- cluster-tutorial-output -->
```
node 2 mode core role follower term 1 leader 1 commit 8 applied 8
cluster 875bd313f7abb7b8c0ca9b6b59e6c292 replication 3 min_isr 1 node_timeout_ms 3000 heartbeat_ms 500 voter_replace_s 600 voters_max 5 mode replicated failover lease read_barrier on lease_s 10 restore_gen 0
group voters 1,2,3 learners -
member 1 up incarnation 1 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard 457ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 2 up incarnation 1 addr 127.0.0.1:17082 peer 127.0.0.1:17092 rack rack2 heard 0ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 3 up incarnation 1 addr 127.0.0.1:17083 peer 127.0.0.1:17093 rack rack3 heard 407ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
volumes 0 users 0
```

Step 6: the client example is the one of the [quick start](../README.md#quick-start-5-minutes-one-machine), pointed at
node 1. Its answer is the same as there. Node 1 has the series, node 2 has none:

<!-- cluster-tutorial-output -->
```
wrote 1440 rows
read 1440 rows, the first: 1717200000000 50.00
24 hours, the first average: 51.28
Oslo day 1: 1320 rows, average 50.23
Oslo day 2: 120 rows, average 47.47
pump7.flow	float32
node 2 has 0 series
```

Step 7: after the kill. Node 2 (a survivor) sees a new term, a leader, and the dead node 1 as `down`:

<!-- cluster-tutorial-output -->
```
node 2 mode core role leader term 2 leader 2 commit 10 applied 10
cluster ... (the settings, as before)
group voters 1,2,3 learners -
member 1 down incarnation 1 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard 5482ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 2 up incarnation 1 addr 127.0.0.1:17082 peer 127.0.0.1:17092 rack rack2 heard 0ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 3 up incarnation 1 addr 127.0.0.1:17083 peer 127.0.0.1:17093 rack rack3 heard 377ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
volumes 0 users 0
```

Your shell may also print a line like `Killed  build/tsdbd ...` about the process you killed. That is the shell, not
tsdb.

Step 8: node 1 is back, as a follower of the new leader, with `incarnation 2`:

<!-- cluster-tutorial-output -->
```
node 1 mode core role follower term 2 leader 2 commit 11 applied 11
cluster ... (the settings, as before)
group voters 1,2,3 learners -
member 1 up incarnation 2 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard 0ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 2 up incarnation 1 addr 127.0.0.1:17082 peer 127.0.0.1:17092 rack rack2 heard 376ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 3 up incarnation 1 addr 127.0.0.1:17083 peer 127.0.0.1:17093 rack rack3 heard 376ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
volumes 0 users 0
```

Step 9: the witness. It is `mode watch`, has no client port (`addr ...:0`) and `volumes_ok 0`: it will never hold a
volume. It has not heard from the others the way a voter does, so `heard` says `never` for them.

<!-- cluster-tutorial-output -->
```
node 4 mode watch role - term 2 leader 2 commit 13 applied 13
cluster ... (the settings, as before)
group voters 1,2,3 learners -
member 1 up incarnation 2 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard never feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier ?
member 2 up incarnation 1 addr 127.0.0.1:17082 peer 127.0.0.1:17092 rack rack2 heard never feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier ?
member 3 up incarnation 1 addr 127.0.0.1:17083 peer 127.0.0.1:17093 rack rack3 heard never feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier ?
member 4 up incarnation 1 addr 127.0.0.1:0 peer 127.0.0.1:17094 rack rack4 heard 0ms feed - voter_ok 1 volumes_ok 0 core_dropped 0 read_barrier on
volumes 0 users 0
```

Step 10 prints what the servers logged about leaders and members. Only the new leader proposes a change, so a line shows
in one log only. For example:

```
tsdbd: cluster: node 2 leads the metadata group (term 2)
tsdbd: cluster: proposes node 1 down (incarnation 1)
tsdbd: cluster: proposes node 1 up (incarnation 2)
```

Why the dead node is `down` only after a few seconds: a server is marked down when the leader has not heard its
heartbeat for `node_timeout_ms` (3 s by default). In that time the survivors also elect the new leader. Step 7's
`wait_for` waits for the `down` line, so by then a new leader exists.

## How to read `status`

`tsdb cluster status PEER` asks one server what it knows and prints one fact per line. Ask any member: they should
agree, up to a moment's delay. A server that has just restarted may show its saved copy first and print a `stale`
line until the group has confirmed it.

| Line | Meaning |
|---|---|
| `node N mode M role R term T leader L commit C applied A` | The server you asked. `N`: its member number (0: in no cluster). `M`: `none` (in no cluster), `core` (it runs a copy of the Raft group: a voter or a learner on its way to be one) or `watch` (it follows the group's state without voting). `R`: `follower`, `pre-candidate`, `candidate` or `leader` (`-` when it is not in the group). `T`: the election term, up by one at every new election. `L`: the leader's number, 0 when it knows none (an election is running). `C`, `A`: how far the group's log is committed, and how far this server has applied it. |
| `stale ...` | This is the copy this server saved before a restart; no one has confirmed it yet. |
| `cluster UUID replication ... lease_s ...` | The cluster's id and its settings (`init` fixes them; see the reference below). |
| `group voters 1,2,3 learners -` | Who votes, and who is catching up to join the vote. |
| `member N STATE incarnation I addr A peer P rack R heard H feed F voter_ok V volumes_ok W core_dropped D read_barrier B` | One line per member. `STATE`: `up`, `down`, `draining` or `out`. `I`: how many times this member has started (it goes up at every restart). `A`: where clients reach it. `P`: where servers reach it. `R`, `F`: its `--rack` and `--power-feed`, `-` for none. `H`: how long ago the server you asked last heard from it. `V`, `W`: whether it may vote and may hold volumes (1/0; a witness has `volumes_ok 0`). `D`: 1 when its Raft core was dropped after a log fault (it goes on as a watcher). `B`: its read barrier setting; `?` when you have not heard it yet. |
| `volume-report ...`, `domain-report ...` | Per member, what it says about its volumes and storage domains. Empty in the preview: no volumes exist. |
| `volumes N users M` | How many volumes and users the group holds. Always 0 in the preview. |

How many voters? The group has one voter with one or two members, three with three or four, and five with five or
more (at most `--voters`). A member that does not vote watches.

## If something goes wrong

- **`cannot connect to ...: Connection refused`**: the server is not running (yet), or the address is wrong. Look at
  `/tmp/tsdb-cluster/n1/log`. A client port or peer port that is taken shows there as "cannot listen"; change the
  numbers in step 3.
- **`... is not a cluster peer port with this secret`**: the address is not a server's peer port (a client port, say),
  or the secret file differs from the one the server was started with.
- **`the secret file ... is open to its group or to others: refused`**: `chmod 600` it. `tsdb cluster secret FILE`
  makes a file with the right mode.
- **`the server is already in a cluster`** (from `init`): that server has a cluster already. To start over, stop it,
  remove its state directory and start it again.
- **`the new server did not answer`** (from `node add`): the server at `NEW_PEER` is not running with `--cluster`
  with the same secret, or it is a member of a cluster already.
- **`--state-dir ... on NFS`**: the cluster's files must be on a local file system.
- **The block fails in the middle and servers keep running**: stop them by process id:
  `for n in 1 2 3 4; do kill $(cat /tmp/tsdb-cluster/n$n/pid); done`. Do not use `pkill` or `killall`: they stop every
  process with that name, also ones that are not yours.
- **Two of the three nodes are gone**: the third has no majority. Its role stays `pre-candidate`, it shows `leader 0`,
  and the dead members stay `up` in its view: nobody is left to mark them down. The group changes nothing until
  one of the others is back. That is the rule, not a bug.
- When you are done: `rm -rf /tmp/tsdb-cluster`.

## Reference

### The server: `tsdbd` options for the cluster

Everything below is off without `--cluster`.

| Option | Meaning |
|---|---|
| `--cluster` | Start the cluster thread: this server can join a metadata group. Needs `--cluster-secret-file` and `--state-dir`, and a writer (not `--read-only` or `--follow`). |
| `--cluster-secret-file F` | The cluster's secret (`tsdb cluster secret F` makes one). Mode 0600, the same file on every server. |
| `--state-dir DIR` | Where the cluster's files live (the Raft log, the snapshot, the server's identity). A local file system, **never** `--dir` and never NFS or CIFS (refused). |
| `--peer-port N\|0` | The port other servers and `tsdb cluster` use. Default: the client port + 1. `0`: let the system choose (with `--port-file`, the file's fourth number). |
| `--peer-listen ADDR` | The address the peer port binds to. Default: `--listen`. |
| `--advertise HOST` | The host other servers and clients are told to use. Default: `--listen`, which must then not be a wildcard (`0.0.0.0`). Needed for a witness. |
| `--rack NAME` | The server's rack, for placing copies in different racks later. Shown by `status`. |
| `--power-feed NAME` | The server's power feed, the same. Shown by `status`. |
| `--bmc ADDR` | The address of the server's management controller, for the `bmc` failover. Stored; nothing uses it yet. |
| `--read-barrier on\|off` | This server's own read barrier setting, for shared volumes. Default `on`. |
| `--witness` | A member without data: needs `--cluster`, takes no `--dir`, needs `--advertise`. Votes and keeps the group's state. |

The client port, `--dir` and every other option work as without `--cluster`. For a script: with `--port-file F` the
file says `PORT PID MPORT PEER_PORT` (`MPORT` is 0 without metrics). Start servers with `--port 0 --peer-port 0
--port-file F`, as the tests do, and a port is never taken twice.

### The command line: `tsdb cluster`

`tsdb cluster` with no arguments prints these. The commands talk to a running server's peer address (`PEER`,
`host:port`). They need no directory and keep no state; they authenticate with the secret
(`--secret-file F`).

| Command | What it does |
|---|---|
| `tsdb cluster secret FILE` | Make a new secret: 32 random bytes as hex, mode 0600. Never overwrites a file. |
| `tsdb cluster init PEER --secret-file F [options]` | Make the cluster on one server that is in no cluster yet. It becomes node 1 and the leader. The options fix the settings (below). |
| `tsdb cluster node add PEER NEW_PEER --secret-file F` | Add the server at `NEW_PEER` (running with `--cluster`, in no cluster yet). `PEER` is any member; the command follows to the leader. A server that is already a member is added again without harm (the same number). |
| `tsdb cluster status PEER --secret-file F` | What that server knows (see above). |

Options of `init`. Each number is a whole number above 0. The settings are stored and shown; in the preview nothing acts on
those about volumes and failover yet.

| Option | Default | Meaning |
|---|---:|---|
| `--replication N` | 3 | Copies of every volume. |
| `--min-isr N` | 1 | Copies that must be in sync for a write to count. At most `--replication`. |
| `--node-timeout-ms MS` | 3000 | A server is marked down after this long without a heartbeat. |
| `--heartbeat-ms MS` | 500 | How often servers report. Below `--node-timeout-ms`. |
| `--isr-lag-ms MS` | 1000 | A copy this far behind leaves the in-sync set. |
| `--out-after-s S` | 1800 | A server down this long is marked `out`. |
| `--voter-replace-s S` | 600 | A voter down this long is replaced by another member. |
| `--voters N` | 5 | The most voters the group has (an even number counts as one less). |
| `--volumes-per-server N` | 16 | Volumes led per server. |
| `--mode replicated\|shared` | replicated | Servers with their own disks, or volumes on shared (NFS) storage. One mode for the whole cluster. |
| `--failover lease\|attested\|bmc` | lease | How a dead owner is replaced in shared mode. |
| `--read-barrier on\|off` | on | Owners answer reads only behind a fenced write (shared mode). |
| `--lease-s S` | 10 | The lease of a shared volume. |

The design lists more commands (`node drain` and `out`, `volume add` and `move`, `user`, `restore`, `adopt` and
others). They are not built: `tsdb cluster` prints what exists.

## What's next

The preview is the first step of a longer plan. The designs are written; the work is in this order:

- **The metadata group, finished** (S1, S2, S4): the proposer's rules for ownership and the time rules, the servers'
  links to each other and routing of requests, users and grants kept in the group, and names that are unique
  across the cluster.
- **Shared mode** (N0 to N8): failover on NFS storage with one writer per volume, fenced by the group (the design of
  [ha.md](ha.md)); this is what production on NFSv4 needs first.
- **Replicated mode** (R1 to R4): each volume on three servers with their own disks, with replication, the in-sync set,
  promotion and rebuild. Raft stays off the insert path.
- Then clients that talk to the owner directly, and a cold tier.

Read more:

- [design-ha-modes.md](design-ha-modes.md): the two modes, which one when, and the build plan with all the step names
  (S0 to S4, N0 to N8, R1 to R4).
- [design-cluster.md](design-cluster.md): the replicated cluster and the metadata group in detail (the `tsdb cluster`
  commands are in its section 12, and design-ha-modes.md section 8.1).
- [ha.md](ha.md): failover on shared NFS storage.
- [roadmap.md](roadmap.md) and the cluster entry of the [changelog](../CHANGELOG.md).
