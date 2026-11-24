# The cluster preview: a tutorial

This page sets up a small tsdb cluster on one machine, looks at it, breaks it on purpose and puts it back together.
It takes about ten minutes. Read the first section before you start: it says what the preview is, and what it is not.

## What this is, and what it is not

Release 0.10.0 has the first piece of tsdb's cluster: the **metadata group**. That is a small group of servers that
agree, with the Raft protocol, on a list of facts about the cluster:

- which servers are **members**, where they are, and whether they are up or down;
- which servers are **voters** (they decide) and which only **watch**;
- the settings of the cluster (how many copies, how long a server may stay quiet);
- the **users** and their **grants** ([Logins](#logins));
- the places where **volumes**, **owners**, **epochs** and **in-sync sets** will be kept.

A **witness** is a member with no data. It only keeps the list and can vote. It is a cheap third or fifth vote.

Release 0.11.0 adds **cluster volumes**: `tsdb cluster volume add` makes a volume that the group places on N servers
(its **copies**, one per rack where it can), and every one of them makes the volume's directory and opens it. Data in
a cluster lives in such volumes only, never in a server's own directory.

What the preview does **not** do yet. Please do not skip this list:

- **It does not copy your data.** A volume has copies on three servers, but a row you write to the volume's leader
  stays on that server: the others stay empty. Writes are off unless the servers start with `TSDB_CLUSTER_WRITES=1`
  (step 3 does), a switch for trying it out that will go once copying works. Step 6 shows it.
- **It does not fail over.** If a server dies, nothing takes over its data. The group notices (it marks the server
  down) and that is all.
- **It does not move volumes.**
- **Clients name the volume.** The client port and the protocol are as without `--cluster`, but a series is made in
  a cluster volume, named like any volume (`tsdbc_series_at` with the volume's name). A request may go to any
  server ([Writes through any server](#writes-through-any-server)). Logins use the cluster's users, the same on
  every server ([Logins](#logins)).
- **It is not for production.** The format of the metadata and the peer protocol may change in a later 0.x
  release. A build of the preview does not talk to a build from before such a change: you start the cluster again
  with `tsdb cluster init`, you do not upgrade it.

Why ship it then? The group is the part everything else stands on, and it is the part that is hardest to get right.
It has its own simulator, a checker and fault tests, and now you can run it yourself. What comes next is at the
[end of this page](#whats-next).

If you do not use `--cluster`, none of this runs, and nothing about your server changes.

## What you need

- A checkout of tsdb, built with `make` (the first step below does it). Linux. No root.
- Three free ports for the servers' client side (17081 to 17083; the witness of step 9 has none) and four for their
  peer side (17091 to 17094), on `127.0.0.1`. If something else uses one of them, change the numbers in the two
  lines marked `# ports` in step 3: they are the only place where the block writes a port.
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
#    Node N listens for clients on port 1708N and for the other servers on port 1709N (the two lines marked ports).
#    --no-auth: no login, to keep the tutorial short ([Logins](#logins) below shows how). Fine on 127.0.0.1, nowhere else.
#    TSDB_CLUSTER_WRITES=1: a preview switch that lets a volume's leader take writes (step 6), though nothing is
#    copied yet. Without it every cluster volume refuses writes.
#    A witness (node 4) has no data directory and no client port.
port() { echo 1708$1; }                    # ports: the client port of node N
pport() { echo 1709$1; }                   # ports: the peer port of node N
peer() { echo 127.0.0.1:$(pport $1); }     # the peer address of node N
start_node() {                             # start_node N [witness]: start a server, remember its process id
    mkdir -p $D/n$1/state
    if [ "${2:-}" = witness ]; then role="--witness --advertise 127.0.0.1"
    else mkdir -p $D/n$1/data; role="--dir $D/n$1/data --no-auth --port $(port $1)"; fi
    TSDB_CLUSTER_WRITES=1 build/tsdbd $role --state-dir $D/n$1/state --peer-port $(pport $1) \
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

# 6. A volume: in a cluster, data lives in volumes the group places, never in a server's own directory. Add one
#    named plant with three copies, one per node, and wait until every node has made its directory (n1/data/v1, ...)
#    and the group has recorded that it opened it (open_gen). Then write a day of readings into plant through its
#    leader (the first of the three; only it takes writes) and read them back. The preview copies nothing yet:
#    the other two copies stay empty.
build/tsdb cluster volume add $(peer 1) plant --replicas 3 --secret-file $D/secret
for n in 1 2 3; do wait_for 1 "^replica volume 1 node $n rid [0-9a-f]* open_gen 1$"; done
status 1 | grep '^volume\|^replica'
leads=$(status 1 | awk '$1 == "volume" && $4 == "plant" { print $8 }')
echo "node $leads leads volume plant"
TSDB_VOLUME=plant build/client-example 127.0.0.1 $(port $leads)
ls -d $D/n[123]/data/v1

# 7. Kill the leader, as a crash would (kill -9, by its process id). The others elect a new leader, and once that
#    one has led for three seconds (node_timeout_ms) without a heartbeat from the dead node, it marks the node down.
#    Ask a survivor.
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
number), the times after `heard`, the term and commit numbers, and which node leads (so which member the kill of step 7 hits, with its
rack). The test compares every other line of this page with the real output.

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
replicated lease_ms 2000 drift_ppm 500 unanswered_ms 1000 commit_interval_ms 5 repl_retain_mb 4096 repl_retain_s 1800 idle_seal_ms 1000 rebuild_mb_s 200 rebuild_max 2 transfer_wait_ms 2000 leader_balance_s 0
group voters 1,2,3 learners -
member 1 up incarnation 1 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard 457ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 2 up incarnation 1 addr 127.0.0.1:17082 peer 127.0.0.1:17092 rack rack2 heard 0ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 3 up incarnation 1 addr 127.0.0.1:17083 peer 127.0.0.1:17093 rack rack3 heard 407ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
volumes 0 users 0
```

Step 6: `volume add` answers with the volume's number. All three nodes hold a copy, node 1 leads it (`leader 1`, the
first of `replicas`), and each copy has a replica id (`rid`, drawn once when its directory is made) and an open
generation (`open_gen`, one more at every start of its server; the group records it before the copy answers anyone).
The client example is the one of the [quick start](../README.md#quick-start-5-minutes-one-machine), pointed at the
volume's leader and told the volume's name (`TSDB_VOLUME=plant`). Its answer is the same as there:

<!-- cluster-tutorial-output -->
```
added volume plant: number 1
volume 1 name plant uuid 6352c65472e57ca48581ca95b719bc8a leader 1 epoch 1 min_isr 1 replicas 1,2,3 isr 1,2,3 isr_version 1 set_version 1 preferred 1
replica volume 1 node 1 rid c4e00215dc777d5a open_gen 1
replica volume 1 node 2 rid cacab39a181a87c0 open_gen 1
replica volume 1 node 3 rid d55421a420347a3b open_gen 1
volumes 1 users 0
node 1 leads volume plant
wrote 1440 rows
read 1440 rows, the first: 1717200000000 50.00
24 hours, the first average: 51.28
Oslo day 1: 1320 rows, average 50.23
Oslo day 2: 120 rows, average 47.47
/tmp/tsdb-cluster/n1/data/v1
/tmp/tsdb-cluster/n2/data/v1
/tmp/tsdb-cluster/n3/data/v1
```

What a cluster node refuses, so that no row ever lands outside a volume the group knows: a series in the server's own
directory (`tsdbc_series` without a volume: `invalid argument`), a local `tsdb DIR volume add` while it runs, and a
start on a directory that already has series or `VOLUMES` lines in it (the server names them and stops). A write to a
copy that does not lead is `UNAVAILABLE` with the reason `fenced`, and a write to any volume of a server started
without `TSDB_CLUSTER_WRITES=1` too. A copy that is older than the group's record of it (`open_gen` above the one in
its directory, say a directory restored from a backup) is refused, naming both numbers.

Step 7: after the kill. Node 2 (a survivor) sees a new term, a leader, and the dead node 1 as `down`:

<!-- cluster-tutorial-output -->
```
node 2 mode core role leader term 2 leader 2 commit 10 applied 10
cluster ... (the settings, as before)
group voters 1,2,3 learners -
member 1 down incarnation 1 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard 5482ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 2 up incarnation 1 addr 127.0.0.1:17082 peer 127.0.0.1:17092 rack rack2 heard 0ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 3 up incarnation 1 addr 127.0.0.1:17083 peer 127.0.0.1:17093 rack rack3 heard 377ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
volume 1 name plant uuid 6352c65472e57ca48581ca95b719bc8a leader 1 epoch 1 min_isr 1 replicas 1,2,3 isr 1,2,3 isr_version 1 set_version 1 preferred 1
replica volume 1 node 1 rid c4e00215dc777d5a open_gen 1
replica volume 1 node 2 rid cacab39a181a87c0 open_gen 1
replica volume 1 node 3 rid d55421a420347a3b open_gen 1
volumes 1 users 0
```

Your shell may also print a line like `Killed  build/tsdbd ...` about the process you killed. That is the shell, not
tsdb.

Step 8: node 1 is back, as a follower of the new leader, with `incarnation 2`. Its copy of plant shows `open_gen 2`
once the group has recorded the new start, a moment later:

<!-- cluster-tutorial-output -->
```
node 1 mode core role follower term 2 leader 2 commit 11 applied 11
cluster ... (the settings, as before)
group voters 1,2,3 learners -
member 1 up incarnation 2 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard 0ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 2 up incarnation 1 addr 127.0.0.1:17082 peer 127.0.0.1:17092 rack rack2 heard 376ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 3 up incarnation 1 addr 127.0.0.1:17083 peer 127.0.0.1:17093 rack rack3 heard 376ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
volume 1 name plant uuid 6352c65472e57ca48581ca95b719bc8a leader 1 epoch 1 min_isr 1 replicas 1,2,3 isr 1,2,3 isr_version 1 set_version 1 preferred 1
replica volume 1 node 1 rid c4e00215dc777d5a open_gen 1
replica volume 1 node 2 rid cacab39a181a87c0 open_gen 1
replica volume 1 node 3 rid d55421a420347a3b open_gen 1
volumes 1 users 0
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
volume 1 name plant uuid 6352c65472e57ca48581ca95b719bc8a leader 1 epoch 1 min_isr 1 replicas 1,2,3 isr 1,2,3 isr_version 1 set_version 1 preferred 1
replica volume 1 node 1 rid c4e00215dc777d5a open_gen 1
replica volume 1 node 2 rid cacab39a181a87c0 open_gen 1
replica volume 1 node 3 rid d55421a420347a3b open_gen 1
volumes 1 users 0
```

Step 10 prints what the servers logged about leaders and members. Only the new leader proposes a change, so a line shows
in one log only. For example:

```
tsdbd: cluster: node 2 leads the metadata group (term 2)
tsdbd: cluster: proposes node 1 down (incarnation 1)
tsdbd: cluster: proposes node 1 up (incarnation 2)
```

Why the dead node is `down` only after a few seconds: a server is marked down when the leader has not heard its
heartbeat for `node_timeout_ms` (3 s by default). The survivors first elect a new leader, and the new leader marks the
node `down` once it has itself led for `node_timeout_ms`, so the `down` comes a few seconds after the election, not at
the moment of the kill. Step 7's `wait_for` waits for the `down` line, so by then a new leader exists.

## Logins

In a cluster the users and their grants belong to the whole cluster, not to one server: you add a user once, and it
can log in on every server. A changed password or a revoked grant counts on every server as soon as the command
answers. Start the servers without `--no-auth`, and give the first superuser to `init`:

```sh
echo 'a long password' > $D/root.pw; echo 'another one' > $D/alice.pw; chmod 600 $D/*.pw
build/tsdb cluster init $(peer 1) --secret-file $D/secret --superuser root --password-file $D/root.pw
build/tsdb cluster user add $(peer 1) alice --password-file $D/alice.pw --secret-file $D/secret
build/tsdb cluster user grant $(peer 2) alice prefix:plant. read --secret-file $D/secret   # any member will do
build/tsdb cluster user list $(peer 3) --secret-file $D/secret
```

A superuser can also manage users over the client protocol (the clients' user calls) on any server; the change is
made in the cluster and answered once every server that is up has it (or after 5 seconds: a server that is slow to
catch up gets it a little later). When the server cannot tell whether the change was made (the cluster's leader
stopped at that moment), it closes the connection instead of answering: look with `tsdb cluster user list` before
you send it again.

Things to know:

- **`tsdb DIR user ...` does not work on a cluster server's directory.** It says so and names `tsdb cluster user`.
- **A server that was down refuses logins for a moment after it starts** (`UNAVAILABLE`): until it has heard the
  cluster, it might still hold a user that was dropped while it was away. The clients do not wait for it: the
  connect fails with that error, and your program tries again (or connects to another server).
- **A server cut off from the others** keeps the users it had until it can reach them again.
- **Servers that had their own users before** (a USERS file in their directory) do not start with `--cluster` until
  that file is gone. Bring their users in first, with the servers stopped:

  ```sh
  build/tsdb cluster user import $(peer 1) /srv/old1/data /srv/old2/data --secret-file $D/secret
  ```

  A user that is the same in every directory is copied as it is (the password too: nobody needs to know it). A user
  that differs between them, or is missing from one, stops the whole import, which lists each such user; add
  `--from-node /srv/old2/data` to say whose copy wins. Running the import again is safe. Then remove each
  directory's USERS file. When every server had its own users, none of them can start yet: move each USERS file
  out of its directory first, start the servers, and import from the moved copies.
- **A server taken out of the cluster** keeps a `CLUSTER_USERS` file in its directory, and `tsdb DIR user ...` goes
  on refusing there until you remove that file by hand.

## Writes through any server

A client does not need to know which server leads a volume. A server that does not lead the volume a request names
passes the request on, one hop, to the volume's leader and relays the answer, so a client with a list of servers
works against any of them (`--forward on`, the default of a `--cluster` server; `--forward off` makes it answer
`MOVED` instead, naming the leader). The rules are short:

- **The user is the same.** The leader checks the grants of the user who logged in at the entry server, as for a
  request sent to it directly.
- **A request is never sent twice.** If the entry server could not write the request to the leader within 2 seconds,
  or it was older than 1 second when its turn came, the answer is `UNAVAILABLE` and nothing was done: send it again.
  If it was written and the link then broke, the entry server closes your connection without an answer: the outcome is
  unknown, as for any broken connection, and your program looks before it sends the write again.
- **`MOVED` carries an address.** When a server answers `MOVED` (forwarding off, or the leader changed while the
  request was on its way), the address in it is the one the cluster's metadata names for the leader. A server that
  restarted on another address is named at the old one until the group has recorded the new one; a client that
  follows `MOVED` in that time connects to nobody or to whatever holds the address now, and the preview's clients check
  no identity after they connect. Keep addresses stable in the preview.
- **Reads on a server that does not lead** are forwarded too, and so are as current as the leader's: replicas do not
  answer reads (`TSDB_REPLICA_READS=1` is a test switch, and its answers may be stale).

What it costs is measured by `build/tsdbc-bench via --host H --port P --via H2:P2 --volume NAME`: the same load sent
to the leader and then to another server, alternating, repeated, with the mean and standard deviation of the requests
a second, the p50 and p99 of writes and reads, and the server CPU per 1000 requests; the hop is the difference of the
two. The cluster's fault test for it is `make jepsen-cluster` with the workload `kv-via`.

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
| `member N STATE incarnation I addr A peer P rack R heard H feed F voter_ok V volumes_ok W core_dropped D read_barrier B` | One line per member. `STATE`: `up` or `down` (the group also knows `draining` and `out`, which nothing sets in the preview). `I`: how many times this member has started (it goes up at every restart). `A`: where clients reach it. `P`: where servers reach it. `R`, `F`: its `--rack` and `--power-feed`, `-` for none. `H`: how long ago the server you asked last heard from it. `V`, `W`: whether it may vote and may hold volumes (1/0; a witness has `volumes_ok 0`). `D`: 1 when its Raft core was dropped after a log fault (it goes on as a watcher). `B`: its read barrier setting; `?` when you have not heard it yet. |
| `replicated lease_ms ...` | The settings of replicated volumes (`init` fixes them; stored, not acted on yet). |
| `volume-report ...`, `domain-report ...` | Per member, what it says about its volumes and storage domains. |
| `volume N name V uuid U leader L epoch E min_isr M replicas A,B,C isr ... isr_version I set_version S [preferred P]` | One line per cluster volume: its number and name, the node that leads it, and the nodes that hold its copies. `preferred P`: the node that should lead it (the first copy in the list); leadership goes back to it once it is in sync again. `I` goes up at every change of the in-sync set, `S` at every change of the set of copies (a copy added, removed or made again). |
| `replica volume N node X rid R open_gen G [replaces Y]` | One line per copy: the copy's replica id (drawn once, when its directory is made; a new id means a new, empty copy) and its open generation (one more at every start of its server, recorded by the group before the copy answers). `replaces Y`: this copy is being built to take the place of node Y's copy (a move); it takes Y's position in the list once Y's copy is removed. |
| `volumes N users M` | How many volumes and users the group holds (users: always 0 in the preview). |
| `moves started S done D cancelled C` | On the group's leader: the copy moves it started, ended with the old copy removed, and ended by `--cancel`. They count from that server's start. |
| `move volume N node A -> node B: STEP, T s` | On the group's leader, one line per move running: its step (`catch-up`, `transfer`, `shrink`, `remove`) and how long it has run. |

How many voters? The group has one voter with one or two members, three with three or four, and five with five or
more (at most `--voters`). A member that does not vote watches.

## If something goes wrong

- **`cannot connect to ...: Connection refused`**: the server is not running (yet), or the address is wrong. Look at
  `/tmp/tsdb-cluster/n1/log`. A client port or peer port that is taken shows there as "cannot listen"; change the
  numbers in the two lines of step 3 marked `# ports`, and nothing else.
- **`... is not a cluster peer port with this secret`**: the address is not a server's peer port (a client port, say),
  or the secret file differs from the one the server was started with.
- **`the secret file ... is open to its group or to others: refused`**: `chmod 600` it. `tsdb cluster secret FILE`
  makes a file with the right mode.
- **`the server is already in a cluster`** (from `init`): that server has a cluster already. To start over, stop it,
  remove its state directory and start it again.
- **`the new server did not answer`** (from `node add`): the server at `NEW_PEER` is not running, or not running
  with `--cluster` and the same secret.
- **`the new server belongs to another cluster or has another id`** (from `node add`): the server at `NEW_PEER` is a
  member of a cluster already. To use it here, stop it, remove its state directory and start it again.
- **`--state-dir ... on NFS`**: the cluster's files must be on a local file system.
- **`... USERS: a cluster server takes its users from the metadata group`**: the server's directory still has a
  USERS file from before it was in a cluster. Bring the users over with `tsdb cluster user import` and remove the
  file ([Logins](#logins)).
- **A login answers `UNAVAILABLE`**: that server has not heard the group since it started, so it does not know yet
  whether a user was dropped meanwhile. It lets logins in once it has caught up; connect again then, or to another
  server (the clients do not retry a login by themselves).
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
| `--wal-sync-ms` | Must stay 0 (sync every write before its answer, the default) with `--cluster`: the server refuses to start otherwise. |
| `TSDB_CLUSTER_WRITES=1` | In the environment, not an option: lets the leader of a cluster volume take writes in the preview, though nothing is copied. Without it every cluster volume refuses writes (`UNAVAILABLE`). |
| `--witness` | A member without data: needs `--cluster`, takes no `--dir` (and has no client port). Give it `--advertise`, the host the others reach it at. It votes and keeps the group's state. |

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
| `tsdb cluster volume add PEER NAME [--replicas N] --secret-file F` | Add a cluster volume called `NAME` on `N` servers (default: the cluster's `--replication`), in different racks where it can. Answers with its number. Refused when fewer than `N` members are up and may hold volumes. A name is never used twice. |
| `tsdb cluster status PEER --secret-file F` | What that server knows (see above). |
| `tsdb cluster volume move PEER VOLUME --log --from A --to B --secret-file F` | Move volume `VOLUME`'s copy on node `A` to node `B` (up, allowed to hold volumes, no copy of it yet). It answers at once and runs in the background: `B` gets a new copy, fills it and joins the in-sync set; then, if `A` leads the volume, leadership moves first; then `A` leaves the in-sync set and its copy is removed, `B` taking its place in the list. The number of in-sync copies never goes below what it was before `B` joined, so a lagging copy makes the move wait. A new group leader carries a move on. One move per volume at a time. With `--cancel` instead, `B`'s copy is removed and the old set stays (a move already past its last step ends anyway). `status` shows it. |
| `tsdb cluster user add\|passwd\|drop PEER NAME --secret-file F` | Add a user (`--superuser`, `--password-file F`), change a password, drop a user, for every server at once ([Logins](#logins)). |
| `tsdb cluster user grant\|revoke PEER NAME SELECTOR RIGHTS --secret-file F` | Give or take rights, as `tsdb DIR user grant` does on one server. |
| `tsdb cluster user list PEER [NAME] --secret-file F` | The users and their grants. |
| `tsdb cluster user import PEER DIR... [--from-node DIR] --secret-file F` | Bring the users of servers that ran on their own into the cluster ([Logins](#logins)). |

Options of `init`. Each number is a whole number above 0. The settings are stored and shown; in the preview nothing acts on
those about volumes and failover yet. Members are only `up` or `down` in the preview: `draining` and `out` are
for later.

| Option | Default | Meaning |
|---|---:|---|
| `--replication N` | 3 | Copies of every volume. |
| `--min-isr N` | 1 | Copies that must be in sync for a write to count. At most `--replication`. With fewer, writes are refused (`UNAVAILABLE`) and reads go on. A write already sent when the set shrinks below it is held until the set grows again: the client sees no answer (its timeout, an unknown outcome), never a refusal. |
| `--node-timeout-ms MS` | 3000 | A server is marked down after this long without a heartbeat. |
| `--heartbeat-ms MS` | 500 | How often servers report. Below `--node-timeout-ms`. |
| `--isr-lag-ms MS` | 1000 | A copy this far behind (or silent this long) leaves the in-sync set, and comes back once it has caught up. At least 500. |
| `--out-after-s S` | 1800 | A server down this long is marked `out`. Stored; in the preview no server is ever marked `out`. |
| `--voter-replace-s S` | 600 | A voter down this long is replaced by another member. |
| `--voters N` | 5 | The most voters the group has (an even number counts as one less). |
| `--volumes-per-server N` | 16 | Volumes led per server. |
| `--mode replicated\|shared` | replicated | Servers with their own disks, or volumes on shared (NFS) storage. One mode for the whole cluster. |
| `--failover lease\|attested\|bmc` | lease | How a dead owner is replaced in shared mode. |
| `--read-barrier on\|off` | on | Owners answer reads only behind a fenced write (shared mode). |
| `--lease-s S` | 10 | The lease of a shared volume. |
| `--lease-ms MS` | 2000 | A replicated volume's leader read lease. |
| `--drift-ppm N` | 500 | The clock drift every lease allows for. |
| `--unanswered-ms MS` | 1000 | A copy kept in sync that does not answer a promotion is dropped after this. |
| `--commit-interval-ms MS` | 5 | The commit point reaches the copies within this. |
| `--repl-retain-mb MB` | 4096 | Log kept for a copy that fell behind, at most, per volume. A copy further behind needs a snapshot. The bound is per volume: a server leading V volumes may keep V times this, and a full disk stops the volume's writes for good (a failed log write is fail-stop), so leave room for it. |
| `--repl-retain-s S` | 1800 | And for at most this long. |
| `--idle-seal-ms MS` | 1000 | An idle leader closes its log segment after this. At least 100. A volume written more slowly than once per this interval gets one small part per write on every copy until merges combine them (a write every 2 s: about 1800 parts an hour), and an idle leader syncs one small log record a second on each copy. Raise it for slow writers. |

The design lists more commands (`node drain` and `out`, `restore`, `adopt` and
others). They are not built: `tsdb cluster` prints what exists.

## What's next

The preview is the first step of a longer plan. The designs are written; the work is in this order:

- **The metadata group, finished** (S4): names that are unique across the cluster. The servers' links to each other,
  routing and one-hop forwarding of requests ([Writes through any server](#writes-through-any-server)) are in this
  preview. The group's state machine (S1) and what a single server does while its storage
  is away (UNAVAILABLE, in N1) are already in 0.10.0, and users and grants kept in the group, with logins on every
  server ([Logins](#logins)), are in this preview; the proposer's rules for ownership and the time rules come with N7.
- **Shared mode** (N0, N2 to N8): a go/no-go measurement (N0), then failover on NFS storage with one writer per volume,
  fenced by the group (the design of [ha.md](ha.md)); this is what production on NFSv4 needs first.
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
