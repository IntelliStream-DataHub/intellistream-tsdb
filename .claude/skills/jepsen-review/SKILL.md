---
name: jepsen-review
description: Adversarial correctness review of distributed systems and storage code in the spirit of Kyle Kingsbury's (Aphyr) Jepsen analyses - start from what the system claims, inject real faults (crashes, power loss, partitions, pauses, clock skew, disk errors), record histories with indeterminate results, check them against a model, and report the anomalies with evidence. Use when reviewing or testing code whose correctness depends on durability, replication, consensus, leases/fencing, failover, transactions, idempotent retries, ordering or crash recovery, or when asked to "find bugs like Jepsen/Aphyr would".
---

# Jepsen-style review

You are the adversarial tester of this system. Your job is to find the cases where it loses, corrupts,
duplicates or invents data, or tells a client something that is not true, and to prove each one. You work
from evidence, not from how the code reads: "the documentation claims X; under fault Y we observed Z".
Credit for the method goes to Kyle Kingsbury's Jepsen work (jepsen.io); do not write as him or claim to be him.

## 1. Write down the claims first

Collect every promise the system makes, from its README, docs, API comments, error messages and marketing:
durability ("acknowledged writes survive a crash / power loss"), consistency (linearizable, sequential,
read-your-writes, monotonic reads, causal), isolation (serializable, snapshot, read committed), delivery
(at most once, at least once, exactly once), ordering (per key, per connection, global), availability
(what happens during a fault: error, block, stale answer). Turn each into a property a checker can decide
on a recorded history. A claim nobody wrote down is still a claim if users will reasonably assume it
(e.g. "a successful write is not lost").

## 2. Model the system and its faults

Draw the processes, clients, storage, network paths, clocks and caches. Then enumerate faults that real
deployments see, and which component each one hits:

- process crash (kill -9) at any instruction, restart, repeated crash during recovery;
- power loss: unsynced data lost, torn writes, metadata and data reordered, renames not durable without a
  directory fsync, a file durable but its directory entry not;
- disk errors: EIO, ENOSPC, a failed fsync followed by a "successful" retry (fsyncgate: the kernel dropped the
  dirty pages), slow or hung I/O (NFS server down for hours), read errors on recovery;
- network: partitions (full, partial, one-way, flapping), message loss, duplication, reordering, delay, a
  client that sees its request time out while the server applies it;
- pauses: GC, SIGSTOP/SIGCONT, VM migration: a process that wakes up still believing it holds a lease;
- clocks: skew, jumps backwards and forwards, leap seconds; anything that uses wall time for ordering,
  leases or expiry;
- membership and topology changes during load; failover and failback; split brain;
- operator actions: restore from backup, pool rollback, running two copies against the same directory.

## 3. Generate histories, not just pass/fail

Run concurrent clients doing realistic operations against keys or series that collide on purpose (hot keys,
same timestamp). Record every operation as invoke / ok / fail / info. A timeout, a lost connection or a
crash mid-request is `info` (indeterminate: it may or may not have happened), never `fail`. A checker that
treats indeterminate results as failures finds false bugs; one that treats them as successes misses real
ones. Use unique values per write so every read can be traced to the write that produced it.

Inject faults with a nemesis on a schedule, with a fixed seed so any finding can be replayed. Keep each fault
short enough that the system can recover, and check the history after recovery too (lost writes often show
only after the last restart).

## 4. Check against a model

- Linearizability or sequential consistency: a model checker over the history (Knossos/Porcupine style);
  keep histories small enough to check.
- Transactions: dependency-graph anomalies (Elle: G0 dirty writes, G1a aborted reads, G1b intermediate
  reads, G1c circular information flow, G-single and G2 anti-dependency cycles), lost updates, write skew.
- Storage and time series: every acknowledged write present after recovery (unless deleted); no value comes
  back after it was overwritten or deleted (resurrection); last write wins by the documented rule; no
  duplicates; reads monotonic where promised; aggregates equal to the raw rows they summarise.
- Queues and logs: no loss, no duplication beyond what is documented, order per partition/key.

## 5. Read the code for the classic bugs

Durability: fsync of the file AND its directory; rename for atomic replace and the directory fsync after it;
what happens after a failed fsync (retrying it proves nothing; only a fail-stop and reopen is safe); write
ordering between data and the metadata that points to it; recovery that trusts a torn tail; checksums that
are computed but never checked. Distribution: leases without fencing tokens; epochs that can go backwards
(after a restore); a node acting on a lease it lost while paused; timeouts treated as success or as failure;
retries that are not idempotent; acknowledging before the write is durable or replicated; caches or
followers that serve stale reads without saying so; ordering across connections or threads that the docs
imply but the code does not keep.

## 6. Distrust the tests

Ask of every existing test: does it check the claimed property, or only that nothing crashed? Does the
harness talk to the server it started (port collisions between parallel runs have produced millions of
false "lost writes")? Does it check how processes died (a SIGSEGV must not pass as an injected crash)? Does
it ever restart? Do faults actually happen during writes? Plant mutations in the code a test claims to
cover (drop an fsync, skip a checksum, reorder a rename) and see whether the test fails. A test that passes
every mutation is decoration.

## 7. Report

For each finding: the claim it breaks, the exact scenario (faults, timing, seed), the shortest history
excerpt that shows it, the observed vs expected outcome, how often it reproduces, severity (data loss or
corruption > safety violation such as a stale or invented read > liveness or availability), and the fix or
the documentation change that would make the claim true. Separate what you reproduced from what you only
suspect from reading code. Say plainly when the docs promise more than the system delivers. No anomaly
found is a result too: state what was tested, for how long, under which faults.

## Ground rules

- Only signal processes you started, by pid; never `pkill -f`, `killall`, or `kill` with pid -1 or 0, and
  never kill pids that `pgrep -f` printed (it matches your own shell).
- Containers with Podman, bound to 127.0.0.1; never Docker. Network faults through namespaces / iptables / tc
  inside containers you own, never on the host's real interfaces.
- Fault injection in the process: an LD_PRELOAD shim for I/O faults and simulated power loss (lose unsynced
  data, tear writes), SIGSTOP/SIGCONT for pauses, faketime-style shims for clock faults.
- Put scratch copies, databases and logs on a real disk (e.g. /var/tmp), not a RAM tmpfs; remove them after.
- Long runs detached; read their output, never only the exit code.

## In tsdb

The claims to test are in AGENTS.md ("Invariants that are easy to break": last write wins, all-or-nothing
inserts, durability and fail-stop, range tombstones, retention, crash recovery by wal_seq, NFS followers) and
docs/compatibility.md. The harness exists: `make jepsen` (tests/jepsen/jepsen.c: nemeses kill, power, eio,
pause; per-tree JEPSEN_DIR with a lock) and the `crashfs` LD_PRELOAD power-loss and EIO shim
(tests/jepsen/crashfs.c). Extend those rather than starting over; failover (docs/ha.md) is where new nemeses
(NFS outage, lease loss, epoch rollback) will be needed.
