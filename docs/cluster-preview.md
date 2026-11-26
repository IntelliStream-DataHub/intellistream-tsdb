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
a cluster lives in such volumes only, never in a server's own directory. The copies are kept equal: the leader sends
every write to the others and answers once all the copies that are in sync have it. When the leader's server dies,
another copy takes over (step 7). A copy that fell too far behind, or lost its disk, is rebuilt from a snapshot of the
leader; copies can be moved to other servers and the leader handed over (step 10).

What the preview does **not** do yet. Please do not skip this list:

- **It does not decide by itself where volumes live.** A server that dies is replaced by another copy of each volume
  it led (step 7), but nothing makes a new copy elsewhere: you move a copy, hand over a leader or rebuild a copy
  yourself (step 10). There is no `node drain` and no automatic rebuild of a lost server's volumes yet; a server is
  never marked `out`.
- **Clients name the volume.** The client port and the protocol are as without `--cluster`, but a series is made in
  a cluster volume, named like any volume (`tsdbc_series_at` with the volume's name). A request may go to any
  server ([Writes through any server](#writes-through-any-server)). Logins use the cluster's users, the same on
  every server ([Logins](#logins)).
- **It is not for production.** The format of the metadata and the peer protocol may change in a later 0.x
  release. A build of the preview does not talk to a build from before such a change: you start the cluster again
  with `tsdb cluster init`, you do not upgrade it. This build's metadata is version 6 and its peer protocol version 10;
  a server that finds a state directory or a peer of another version stops, or refuses the peer, with a line naming both
  versions. The changes of replica sets, moves and rebuilds (R4) are in those versions, so a cluster made before them
  must be made again.

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
#    A witness (node 4) has no data directory and no client port.
port() { echo 1708$1; }                    # ports: the client port of node N
pport() { echo 1709$1; }                   # ports: the peer port of node N
peer() { echo 127.0.0.1:$(pport $1); }     # the peer address of node N
start_node() {                             # start_node N [witness]: start a server, remember its process id
    mkdir -p $D/n$1/state
    if [ "${2:-}" = witness ]; then role="--witness --advertise 127.0.0.1"
    else mkdir -p $D/n$1/data; role="--dir $D/n$1/data --no-auth --port $(port $1)"; fi
    build/tsdbd $role --state-dir $D/n$1/state --peer-port $(pport $1) \
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
#    leader (only the leader takes writes directly; it replicates every write to the other two copies before answering,
#    so all three copies hold the data) and read them back.
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

# 10. Hand over, move, rebuild. Node 5 is a fourth data server. Volume 1 (plant) has its copies on 1, 2 and 3.
#     a. transfer: plant's leadership goes to another copy. Writes wait a moment while the old leader sends all it has to
#        the new one, nothing is lost. The command answers when the new leader is in place.
#     b. move: plant's copy on one node moves to node 5, in the background. wait_gone waits for the old copy to go.
#     c. reset: a copy whose disk is lost is refused at its start, with a line that names the command that fixes it.
#        The reset gives it a new identity (a new rid) and the volume's leader sends it a snapshot of the volume.
wait_gone() {                              # wait_gone N REGEX: wait (30 s at most) until node N's status has no such line
    for i in $(seq 150); do
        s=$(status $1 2>/dev/null) && echo "$s" | grep -q '^volumes ' && ! echo "$s" | grep -q "$2" && return 0
        sleep 0.2
    done
    echo "still there: $2"; return 1
}
wait_for 1 '^volume 1 .* isr 1,2,3 '       # the restarted node has caught up and is in sync again
start_node 5
wait_for 5 '^node'
build/tsdb cluster node add $(peer 1) $(peer 5) --secret-file $D/secret
wait_for 1 '^member 5 up .* volumes_ok 1 '
to=$(status 1 | awk '$1 == "volume" && $4 == "plant" { n = split($16, a, ","); for (i = 1; i <= n; i++) if (a[i] != $8) { print a[i]; exit } }')
build/tsdb cluster volume transfer $(peer 1) 1 $to --secret-file $D/secret
wait_for 1 "^volume 1 name plant .* leader $to "
status 1 | grep '^volume 1 '
old=$(( to % 3 + 1 ))                      # a third copy, to move away; the last one stays and is reset below
lost=$(( 6 - to - old ))
build/tsdb cluster volume move $(peer 1) 1 --log --from $old --to 5 --secret-file $D/secret
wait_for 1 '^replica volume 1 node 5 '
wait_gone 1 "^replica volume 1 node $old "
status 1 | grep '^volume 1 \|^replica '
kill $(cat $D/n$lost/pid)                  # the disk of node $lost is lost: stop it, remove the volume's directory, start it
while kill -0 $(cat $D/n$lost/pid) 2>/dev/null; do sleep 0.1; done
wait_gone 1 "^volume 1 .* isr [0-9,]*$lost"
rm -rf $D/n$lost/data/v1
start_node $lost
wait_for $lost '^node'
for i in $(seq 150); do grep -q 'tsdb cluster replica reset' $D/n$lost/log && break; sleep 0.2; done
grep -h 'tsdb cluster replica reset' $D/n$lost/log
build/tsdb cluster replica reset $(peer 1) 1 $lost --secret-file $D/secret
wait_for 1 "^volume 1 .* isr [0-9,]*$lost"
status 1 | grep '^volume 1 \|^replica '
grep -h 'snapshot' $D/n$lost/log
ls -d $D/n$lost/data/v1/*.part | head -n 2

# 11. What the servers said about the leaders and the members' state, from their logs (the details differ on your machine).
grep -h 'leads the metadata group\|proposes node' $D/n[123]/log

# 12. Stop every server by its process id, wait until they are gone, and clean up.
for n in 1 2 3 4 5; do kill $(cat $D/n$n/pid); done
for n in 1 2 3 4 5; do while kill -0 $(cat $D/n$n/pid) 2>/dev/null; do sleep 0.1; done; done
rm -rf $D
```

## What you should see

The lines below come from a real run. Yours differ in the places the test ignores: the cluster's id (a random
number), the times after `heard`, the term and commit numbers, which node leads (so which member the kill of step 7
hits, with its rack) and the numbers of the volume's copies, its epoch and in-sync set. The test compares every other line
of this page with the real output.

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

and `status 2` shows the cluster, asked of node 2, which follows. The last two lines count what the group's leader has
done with leadership transfers and copy moves (step 10). They count from the start of the server you ask, so they are 0
here:

<!-- cluster-tutorial-output -->
```
node 2 mode core role follower term 1 leader 1 commit 8 applied 8
cluster 875bd313f7abb7b8c0ca9b6b59e6c292 replication 3 min_isr 1 node_timeout_ms 3000 heartbeat_ms 500 voter_replace_s 600 voters_max 5 mode replicated failover lease read_barrier on lease_s 10 restore_gen 0
replicated lease_ms 2000 drift_ppm 500 unanswered_ms 1000 commit_interval_ms 5 repl_retain_mb 4096 repl_retain_s 1800 idle_seal_ms 1000 rebuild_mb_s 200 rebuild_max 2 transfer_wait_ms 2000 leader_balance_s 0
group voters 1,2,3 learners -
member 1 up incarnation 1 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard 344ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 2 up incarnation 1 addr 127.0.0.1:17082 peer 127.0.0.1:17092 rack rack2 heard 0ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 3 up incarnation 1 addr 127.0.0.1:17083 peer 127.0.0.1:17093 rack rack3 heard 242ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
volumes 0 users 0
transfers started 0 done 0 timed_out 0 cancelled 0 fell_back 0 balance 0 drains_timed_out 0 resumed_blind 0 ready 0 cuts 0
moves started 0 done 0 cancelled 0
```

Step 6: `volume add` answers with the volume's number. All three nodes hold a copy, node 1 leads it (`leader 1`, the
first of `replicas`), and each copy has a replica id (`rid`, drawn once when its directory is made) and an open
generation (`open_gen`, one more at every start of its server; the group records it before the copy answers anyone).
`isr 1,2,3` is the in-sync set: the copies that have everything the leader has acknowledged. A row you write to the
leader is on every copy of the set before the write is answered. The `volume-report` lines are what each member says
about its copy: `role none` is a copy that follows. The client example is the one of the
[quick start](../README.md#quick-start-5-minutes-one-machine), pointed at the volume's leader and told the volume's
name (`TSDB_VOLUME=plant`). Its answer is the same as there:

<!-- cluster-tutorial-output -->
```
added volume plant: number 1
volume-report node 1 volume 1 role none health ok epoch 1 domain - local_fault_s 0 fenced_age_ms 0
volume-report node 2 volume 1 role none health ok epoch 1 domain - local_fault_s 0 fenced_age_ms 0
volume-report node 3 volume 1 role none health ok epoch 1 domain - local_fault_s 0 fenced_age_ms 0
volume 1 name plant uuid ec299dbebada77d5705b89cd49705585 leader 1 epoch 1 min_isr 1 replicas 1,2,3 isr 1,2,3 isr_version 1 set_version 1 preferred 1 redundancy replicas=3
replica volume 1 node 1 rid e6e41ec318c17a90 open_gen 1
replica volume 1 node 2 rid f37918561ea1bb09 open_gen 1
replica volume 1 node 3 rid d7561d069104510c open_gen 1
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
start on a directory that already has series or `VOLUMES` lines in it (the server names them and stops). A client's write to a
copy that does not lead is forwarded to the leader (`MOVED` with `--forward off`; see "Writes through any server"). A copy that is older than the group's record of it (`open_gen` above the one in
its directory, say a directory restored from a backup) is refused, naming both numbers.

Step 7: after the kill. Node 2 (a survivor) sees a new term, a leader and the dead node 1 as `down`. If the node you
killed led plant (node 1 here), the survivors have also promoted another copy: `leader 2 epoch 2`, and the dead node's
own report says `role owner` for as long as it is down. Every new leader gets a higher epoch, and a leader of an
older epoch is refused by the others, so it can never take a write again. Node 1 stays in the in-sync set for
`isr_lag_ms` (1 s by default) and then leaves it:

<!-- cluster-tutorial-output -->
```
node 2 mode core role follower term 2 leader 3 commit 15 applied 15
cluster ... (the settings, as before)
group voters 1,2,3 learners -
member 1 down incarnation 1 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard 4702ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
volume-report node 1 volume 1 role owner health ok epoch 1 domain - local_fault_s 0 fenced_age_ms 0
member 2 up incarnation 1 addr 127.0.0.1:17082 peer 127.0.0.1:17092 rack rack2 heard 0ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
volume-report node 2 volume 1 role none health ok epoch 1 domain - local_fault_s 0 fenced_age_ms 0
member 3 up incarnation 1 addr 127.0.0.1:17083 peer 127.0.0.1:17093 rack rack3 heard 5ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
volume-report node 3 volume 1 role none health ok epoch 1 domain - local_fault_s 0 fenced_age_ms 0
volume 1 name plant uuid ec299dbebada77d5705b89cd49705585 leader 2 epoch 2 min_isr 1 replicas 1,2,3 isr 1,2,3 isr_version 2 set_version 1 preferred 1 redundancy replicas=3
replica volume 1 node 1 rid e6e41ec318c17a90 open_gen 1
replica volume 1 node 2 rid f37918561ea1bb09 open_gen 1
replica volume 1 node 3 rid d7561d069104510c open_gen 1
volumes 1 users 0
transfers started 0 done 0 timed_out 0 cancelled 0 fell_back 0 balance 0 drains_timed_out 0 resumed_blind 0 ready 0 cuts 0
moves started 0 done 0 cancelled 0
```

Your shell may also print a line like `Killed  build/tsdbd ...` about the process you killed. That is the shell, not
tsdb.

Step 8: node 1 is back, as a follower of the new leader, with `incarnation 2`. Its copy of plant shows `open_gen 2`
once the group has recorded the new start, a moment later. It has to catch up from the new leader before it is in the
in-sync set again; step 10 waits for that:

<!-- cluster-tutorial-output -->
```
node 1 mode core role follower term 2 leader 3 commit 17 applied 17
cluster ... (the settings, as before)
group voters 1,2,3 learners -
member 1 up incarnation 2 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard 0ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
member 2 up incarnation 1 addr 127.0.0.1:17082 peer 127.0.0.1:17092 rack rack2 heard 679ms feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier on
volume-report node 2 volume 1 role promoting health ok epoch 2 domain - local_fault_s 0 fenced_age_ms 0
member 3 up incarnation 1 addr 127.0.0.1:17083 peer 127.0.0.1:17093 rack rack3 heard never feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier ?
volume 1 name plant uuid ec299dbebada77d5705b89cd49705585 leader 2 epoch 2 min_isr 1 replicas 1,2,3 isr 2,3 isr_version 3 set_version 1 preferred 1 redundancy replicas=3
replica volume 1 node 1 rid e6e41ec318c17a90 open_gen 1
replica volume 1 node 2 rid f37918561ea1bb09 open_gen 1
replica volume 1 node 3 rid d7561d069104510c open_gen 1
volumes 1 users 0
transfers started 0 done 0 timed_out 0 cancelled 0 fell_back 0 balance 0 drains_timed_out 0 resumed_blind 0 ready 0 cuts 0
moves started 0 done 0 cancelled 0
```

Step 9: the witness. It is `mode watch`, has no client port (`addr ...:0`) and `volumes_ok 0`: it will never hold a
volume. It has not heard from the others the way a voter does, so `heard` says `never` for them.

<!-- cluster-tutorial-output -->
```
node 4 mode watch role - term 0 leader 0 commit 0 applied 18
cluster ... (the settings, as before)
group voters 1,2,3 learners -
member 1 up incarnation 2 addr 127.0.0.1:17081 peer 127.0.0.1:17091 rack rack1 heard never feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier ?
member 2 up incarnation 1 addr 127.0.0.1:17082 peer 127.0.0.1:17092 rack rack2 heard never feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier ?
member 3 up incarnation 1 addr 127.0.0.1:17083 peer 127.0.0.1:17093 rack rack3 heard never feed - voter_ok 1 volumes_ok 1 core_dropped 0 read_barrier ?
member 4 up incarnation 1 addr 127.0.0.1:0 peer 127.0.0.1:17094 rack rack4 heard 0ms feed - voter_ok 1 volumes_ok 0 core_dropped 0 read_barrier ?
volume 1 name plant uuid ec299dbebada77d5705b89cd49705585 leader 2 epoch 2 min_isr 1 replicas 1,2,3 isr 2,3 isr_version 3 set_version 1 preferred 1 redundancy replicas=3
replica volume 1 node 1 rid e6e41ec318c17a90 open_gen 1
replica volume 1 node 2 rid f37918561ea1bb09 open_gen 1
replica volume 1 node 3 rid d7561d069104510c open_gen 1
volumes 1 users 0
transfers started 0 done 0 timed_out 0 cancelled 0 fell_back 0 balance 0 drains_timed_out 0 resumed_blind 0 ready 0 cuts 0
moves started 0 done 0 cancelled 0
```

Step 10: three things an operator does by hand, on the volume plant. The node numbers below are from one run: the
block picks them from what `status` says, so yours differ.

- `volume transfer` hands the leadership to another copy that is in the in-sync set. It answers when the new leader
  has taken over: here node 1 leads again, at epoch 3, after 400 ms. Writes sent to the old leader meanwhile
  answered `UNAVAILABLE` (with a short time to wait) and were not applied, so a client that retries loses nothing.
- `volume move` takes node 2's copy away and gives node 5 one. It answers at once and does the work in the
  background: node 5 gets a new copy and fills it from the leader, joins the in-sync set, and only then is node 2's
  copy removed. The new copy takes the old one's place in `replicas`, so the first copy (`preferred`) does not change by
  moving another. The block waits until node 2's `replica` line is gone; node 2 then deletes its copy.
- `replica reset` is for a copy whose disk is lost. The block stops node 3, waits until the group has dropped it
  from the in-sync set, removes its directory for the volume (`v1`) and starts it again: the server says in its log
  that the directory is missing, will not make a new one by itself (an unmounted disk looks the same) and names the command. The reset
  gives the copy a new replica id (`rid`), and the leader sends the new copy a snapshot of the volume: a directory with
  every part and the log still needed, copied and checked file by file (`5 files, 4054 bytes`), swapped in and
  opened. The log lines show it, and the new copy joins the in-sync set. A directory that was still there is not
  deleted: it is set aside as `v1.lost-<old rid>` (for a server that was down for the reset: `v1.stale-<old rid>`) for
  you to look at and remove.

<!-- cluster-tutorial-output -->
```
added 127.0.0.1:17095
node 1 leads at epoch 3 (400 ms, READY at 1448)
volume 1 name plant uuid ec299dbebada77d5705b89cd49705585 leader 1 epoch 3 min_isr 1 replicas 1,2,3 isr 1,2,3 isr_version 5 set_version 1 preferred 1 redundancy replicas=3
move of volume 1 from node 2 to node 5: proposed (replica 70336e503a1df25e); it runs in the background: learner, join, shrink, remove
volume 1 name plant uuid ec299dbebada77d5705b89cd49705585 leader 1 epoch 3 min_isr 1 replicas 1,5,3 isr 1,3,5 isr_version 7 set_version 3 preferred 1 redundancy replicas=3
replica volume 1 node 1 rid e6e41ec318c17a90 open_gen 2
replica volume 1 node 5 rid 70336e503a1df25e open_gen 1
replica volume 1 node 3 rid d7561d069104510c open_gen 1
tsdbd: volumes: volume 1: /tmp/tsdb-cluster/n3/data/v1 is missing, while the metadata group recorded open generation 1 of replica d7561d069104510c here: not made again by itself (an unmounted disk looks the same); `tsdb cluster replica reset PEER 1 3` makes a fresh replica
volume 1: reset of node 3 proposed (new instance 62f89e2e8f926b65); its old directory is set aside as v1.lost-<old rid> and a fresh one fills
volume 1 name plant uuid ec299dbebada77d5705b89cd49705585 leader 1 epoch 3 min_isr 1 replicas 1,5,3 isr 1,3,5 isr_version 9 set_version 4 preferred 1 redundancy replicas=3
replica volume 1 node 1 rid e6e41ec318c17a90 open_gen 2
replica volume 1 node 5 rid 70336e503a1df25e open_gen 1
replica volume 1 node 3 rid 62f89e2e8f926b65 open_gen 2
tsdbd: replication: volume 1: the leader (node 1) no longer has the records this replica needs: it needs a snapshot and asks the leader for one
tsdbd: replication: volume 1: snapshot received from node 1: 5 files, 4054 bytes in 2.2 s (0.0 MB/s)
tsdbd: replication: volume 1: snapshot installed: P 1461 of epoch 3, REPL fence 3 (it was 3), open generation 1, in 0.0 s; the volume opens again
/tmp/tsdb-cluster/n3/data/v1/p79500_1_1_0.part
```

Step 11 prints what the servers logged about leaders and members. Only the new leader proposes a change, so a line
shows in one log only. For example:

```
tsdbd: cluster: node 3 leads the metadata group (term 2)
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
| `cluster UUID replication ... mode M failover F read_barrier B lease_s ... restore_gen R` | The cluster's id, settings (`init` fixes them; see the reference below) and restore generation. |
| `group voters 1,2,3 learners -` | Who votes, and who is catching up to join the vote. |
| `member N STATE incarnation I addr A peer P rack R heard H feed F voter_ok V volumes_ok W core_dropped D read_barrier B` | One line per member. `STATE`: `up` or `down` (the group also knows `draining` and `out`, which nothing sets in the preview). `I`: how many times this member has started (it goes up at every restart). `A`: where clients reach it. `P`: where servers reach it. `R`, `F`: its `--rack` and `--power-feed`, `-` for none. `H`: how long ago the server you asked last heard from it. `V`, `W`: whether it may vote and may hold volumes (1/0; a witness has `volumes_ok 0`). `D`: 1 when its Raft core was dropped after a log fault (it goes on as a watcher). `B`: its read barrier setting; `?` when you have not heard it yet. |
| `replicated lease_ms ...` | The settings of replicated volumes (`init` fixes them; see the reference below). |
| `volume-report ...`, `domain-report ...` | Per member, what it says about its volumes and storage domains. |
| `volume N name V uuid U leader L epoch E min_isr M replicas A,B,C isr ... isr_version I set_version S [preferred P] redundancy ...` | One line per cluster volume: its number and name, the node that leads it, the nodes that hold its copies, and its redundancy (`replicas=N` or `ec=K+M stripe ... stripe_version V gc_suspended 0\|1`). `preferred P`: the node that should lead it (the first copy in the list); leadership goes back to it once it is in sync again. `I` goes up at every change of the in-sync set, `S` at every change of the set of copies (a copy added, removed or made again). |
| `replica volume N node X rid R open_gen G [replaces Y]` | One line per copy: the copy's replica id (drawn once, when its directory is made; a new id means a new, empty copy) and its open generation (one more at every start of its server, recorded by the group before the copy answers). `replaces Y`: this copy is being built to take the place of node Y's copy (a move); it takes Y's position in the list once Y's copy is removed. |
| `volumes N users M` | How many volumes and users the group holds. |
| `transfers started S done D timed_out T cancelled C fell_back F balance B drains_timed_out D2 resumed_blind R ready Y cuts K` | Counters of leadership transfers, from the start of the server you ask: the ones the group's leader started and how they ended, `balance` of them started by the leader balance, and, on the server that led the volume, drains that gave up (`drains_timed_out`), records it accepted after the hard limit of a transfer's wait, 10 times `transfer_wait_ms` (`resumed_blind`: they may be cut), READYs sent, and log records a replica cut. |
| `moves started S done D cancelled C` | On the group's leader: the copy moves it started, ended with the old copy removed, and ended by `--cancel`. They count from that server's start. |
| `move volume N node A -> node B: STEP, T s` | On the group's leader, one line per move running: its step (`catch-up`, `transfer`, `shrink`, `remove`) and how long it has run. |
| `peer links KIND: none N header N full N tcp N rdma N` | Active data-link counts by kind, check level (`none`, `header`, `full`), and transport (`tcp`, `rdma`). |

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
| `TSDB_CLUSTER_WRITES=1` | In the environment, not an option, for tests only: opens every cluster volume as a plain writer, with no replication, no failover and none of step 10. Do not set it. |
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
| `tsdb cluster volume add PEER NAME --redundancy ec=K+M --secret-file F` | An erasure-coded volume (K 1 to 16, M 1 to 4): its settled parts are coded K+M across a stripe group of K+M members, each with a chunk store up (`tsdbd --chunk-dir`), which the leader picks once; positions are never renumbered. Its log is kept on M+1 servers and `min_isr` is M. Refused, naming how many members qualify, when fewer than K+M do. `--redundancy replicas=N` is `--replicas N`. `tsdb cluster status` shows `redundancy ec=K+M stripe A,B,... stripe_version V gc_suspended 0\|1`. |
| `tsdb cluster volume gc resume PEER VOLUME --secret-file F` | Turn the orphan collection of an erasure-coded volume (a name, else a number) back on. A `force-leader` (and a restore, once built) stops it (`gc_suspended 1`), because a chunk that a lost log tail still needs would look like an orphan; resume it once the volume is checked. |
| `tsdb cluster status PEER --secret-file F` | What that server knows (see above). |
| `tsdb cluster volume move PEER VOLUME --log --from A --to B --secret-file F` | Move volume `VOLUME`'s copy on node `A` to node `B` (up, allowed to hold volumes, no copy of it yet). It answers at once and runs in the background: `B` gets a new copy, fills it and joins the in-sync set; then, if `A` leads the volume, leadership moves first; then `A` leaves the in-sync set and its copy is removed, `B` taking its place in the list. The number of in-sync copies never goes below what it was before `B` joined, so a lagging copy makes the move wait. A new group leader carries a move on, but once `B` has joined it waits until every other copy is in sync (it no longer knows the smaller size from before the join), so with another copy down such a move waits for it to come back. One move per volume at a time. With `--cancel` instead, `B`'s copy is removed and the old set stays (a move already past its last step ends anyway). `status` shows it. |
| `tsdb cluster volume transfer PEER VOLUME NODE --secret-file F` | Hand the leadership of volume number `VOLUME` to `NODE`, a copy that is in the in-sync set and up. The leader stops taking writes (they answer `UNAVAILABLE`, retry after about 50 ms: nothing was done), waits until `NODE` has everything it has written (at most `transfer_wait_ms`), and the group then makes `NODE` the leader under a higher epoch. The command answers when `NODE` leads: `node N leads at epoch E (T ms, READY at X)`. A transfer that cannot finish is cancelled and the old leader goes on; refused at once, with the reason, when `NODE` already leads, is not in the in-sync set or not up, the old leader is not up, or another transfer or a promotion of the volume is under way. |
| `tsdb cluster replica reset PEER VOLUME NODE --secret-file F` | For a copy whose directory is lost, or that you do not trust: give the copy on `NODE` a new replica id and rebuild it from a snapshot of the leader. Allowed only for a copy that neither leads nor is in the in-sync set (the leader or a copy that is in sync is refused, with the reason). A directory that is still there is set aside as `v<N>.lost-<old rid>` (as `v<N>.stale-<old rid>` when its server was down for the reset) and never deleted by tsdb; a missing one is simply made again. Never done by itself: a server whose directory for a volume is missing starts, says so in its log and names this command, because an unmounted disk looks the same. |
| `tsdb cluster user add\|passwd\|drop PEER NAME --secret-file F` | Add a user (`--superuser`, `--password-file F`), change a password, drop a user, for every server at once ([Logins](#logins)). |
| `tsdb cluster user grant\|revoke PEER NAME SELECTOR RIGHTS --secret-file F` | Give or take rights, as `tsdb DIR user grant` does on one server. |
| `tsdb cluster user list PEER [NAME] --secret-file F` | The users and their grants. |
| `tsdb cluster user import PEER DIR... [--from-node DIR] --secret-file F` | Bring the users of servers that ran on their own into the cluster ([Logins](#logins)). |

Options of `init`. Each number is a whole number above 0. The settings are fixed when the cluster is made and shown by
`status`; changing one on a running cluster is not built (to change one, make the cluster again). Those about
replication, leases, rebuilds and transfers are acted on; `--out-after-s`, `--volumes-per-server`, `--lease-s` and `--read-barrier` are only
stored, and `--mode shared` and `--failover` (the shared side of the cluster) are not built. Members are only `up` or `down` in the preview: `draining` and `out` are for later.

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
| `--rebuild-mb-s MB` | 200 | How fast a server sends a snapshot (a rebuilt copy's files), per link, in megabytes a second. The limit is on the sending side: a receiving server is not limited, and a server that sends to several others uses this rate for each. Snapshots have a link of their own, so a write's files never wait behind one. |
| `--rebuild-max N` | 2 | Snapshots a server sends at once, and, counted apart, snapshots it receives at once (at most 16). Another waits and asks again. A receiving server also needs free space on the disk of the volume for the whole snapshot plus a tenth of it (and at least 1 GiB more), because the old directory stays until the new one is in place; otherwise it refuses the copy and says so in its log, and asks again later. |
| `--transfer-wait-ms MS` | 2000 | How long a leader drains for a transfer (100 to 60000): the time writes wait, at most, before it goes on as leader. |
| `--leader-balance-s S` | 0 | 0: off. Otherwise a volume whose first copy (`preferred`) is not its leader is handed back to that copy, by a transfer, once the copy has been in the in-sync set for this long (at most 86400). One transfer at a time on a server and one a second in the cluster; after a failed one the copy is left alone for `S`, doubling up to 10 minutes. Off by default until the pause of a transfer on a busy volume has been measured on real servers (the test-machine figures are in design-cluster.md 3.2). |

The design lists more commands (`node drain` and `out`, `restore`, `adopt` and
others). They are not built: `tsdb cluster` prints what exists.

## What's next

The preview is the first step of a longer plan. The designs are written; the work is in this order:

- **The metadata group, finished** (S4): names that are unique across the cluster. The servers' links to each other,
  routing and one-hop forwarding of requests ([Writes through any server](#writes-through-any-server)) are in this
  preview. The group's state machine (S1) and what a single server does while its storage
  is away (UNAVAILABLE, in N1) are already in 0.10.0, and users and grants kept in the group, with logins on every
  server ([Logins](#logins)), are in this preview; the proposer's rules for ownership and the time rules come with N7.
- **Replicated mode (R1 to R4) and erasure coding (R5 to R8) are built**: each volume on servers with their own
  disks, with replication, the in-sync set, promotion, transfer, move, snapshot rebuild, and erasure-coded settled
  parts. Raft stays off the insert path.
- **Shared mode** (N0, N2 to N8): a go/no-go measurement (N0), then failover on NFS storage with one writer per volume,
  fenced by the group (the design of [ha.md](ha.md)); this is what production on NFSv4 needs first.
- Then clients that talk to the volume leader directly, and device classes / storage tiers (R10, R14).

Read more:

- [design-ha-modes.md](design-ha-modes.md): the two modes, which one when, and the build plan with all the step names
  (S0 to S4, N0 to N8, R1 to R4).
- [design-cluster.md](design-cluster.md): the replicated cluster and the metadata group in detail (the `tsdb cluster`
  commands are in its section 12, and design-ha-modes.md section 8.1).
- [ha.md](ha.md): failover on shared NFS storage.
- [roadmap.md](roadmap.md) and the cluster entry of the [changelog](../CHANGELOG.md).
