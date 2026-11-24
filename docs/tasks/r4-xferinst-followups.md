# A received snapshot's install (server/snapinst_run.c, rsnap.c, cvol.c): what r4-xferinst left open

**Open (from the r4-xferinst review, 2026-11-24). Owner: R4 (r4-transfer or a later R4 track).**

## A volume with no driver never asks for a snapshot

`cvol_install` installs into a volume with no engine (MV_WAIT, MV_REFUSED) as well, with the REPL on disk as `keep`.
But only a driver asks for a copy (`rsnap_need` and `rsnap_rebuild` take its `rvol`), so that path is taken only by a
copy that completes after its driver was closed and before the volume opened again (a set change or a reset during the
transfer). A fresh learner opens an empty engine and asks from it, which is the path with an engine; a volume refused
at start never asks, and stays refused until an operator acts. No test drives the path with no engine (the review's
mutant `noenginepath`, which refuses it, passes every scenario). What is needed: a requester without a driver (the
manager, for a refused volume whose record names a leader), and a scenario that installs into a refused volume.

## A copy whose log end is unknown after the close is refused

The check after the close compares P with the log end the joined driver left (`repld_close_end`). A driver that ended
with no core (a failed demote) leaves none, and the install is then refused and asked for again. Such a driver does
not ask again either (a failed replica asks only when DIVERGED), so in practice this ends with the volume's restart.

## A swap that is done but whose cleanup failed counts as failed

When `snap_install` fails after the swap (the removal of `v<N>.old` fails), `v<N>` is already the new copy, but the
install answers an error: `snap.failed` goes up and "snapshot installed" is not logged. The prepare after the hand-back
finishes it (`snap_leftovers` removes `v<N>.old`). Only the counters are wrong; a fault hook in `inst_swap`'s cleanup
would let a test see it.

## The heartbeat says OK during an install

While MV_INSTALL and MV_REPREP the heartbeat reports the volume OK with its old open generation, a DIVERGED replica
being rebuilt too. It is out of the ISR, so nothing counts on it, but STATUS and the topology say OK, not RECOVERING.

## The install time of a large copy is not measured

`snap.install_last_s` and `tsdb_snapshot_install_last_seconds` hold the last install's time; test_cluster measures only
small volumes (snapcatch: well under a second). An install reads every byte of the copy on the one snapshot worker
(D18) and blocks every other transfer of the node for that time; a number for a volume of tens of GB is still to be
taken (`snaplarge` builds such a copy, and could install it).

## Item 6 cannot be told apart yet

`log_start_check` calls `repl_replay_start(rv->core, replica_replay_start(db))`, but `replica_replay_start` is still
the stub that answers the log start, so no test can tell the call from the old behaviour. r4-replay, which makes it
real, adds the test.
