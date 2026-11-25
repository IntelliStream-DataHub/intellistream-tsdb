# The margins of cluster volumes (server/margins.c): what r8o-margins left open

**Open (from the r8o-margins review, 2026-11-25). Owner: R8 (r8f, r8n) and R9.**

## The settled margin counts members, not stores

`margins_of` counts a stripe position present while its member is UP or DRAINING in the metadata copy. A member that
heartbeats UP while its `--chunk-dir` store failed in service (`chunk_store_failed`), opened DOWN (missing, damaged
META, EIO), or came back empty (design-cluster-data.md E15's second LOST cause) still counts. So STATUS and the metrics
port say `settled_margin=2, tier=ok` when a disk, not a node, was lost. The plan (D20.2, E24) asks for the fewest
positions beyond k over the volume's objects "from the leader's report": the leader's cold ops know which positions
answer per object (r8f, r8n). Until then docs/metrics.md and docs/ha.md say "members, not their chunk stores". Owner's
decision pending whether that is acceptable for R8's release or must be built before it.

## The group's margin is the objects' minimum only while every object has all k + m positions

The object record's `positions` is 0 today (ecpart.h). R9's degraded settle (E10: k + m - 1 positions, margin m - 1 at
birth) and E28's replacement positions that are not yet whole give an object a lower margin than its group's. R9 must
take the minimum over objects (or over the classes of objects the leader tracks).

## The corrupt state and the LOST answer wait for real settled reads

Only the test hook TSDBD_TEST_COLD marks a read as settled (`unav_cold` in tsdbd). r8n's read seam must set it around
the replies of a settled read: UNAVAILABLE short of k positions (named LOST or UNREACHABLE by the tier) and CORRUPT
(which marks `vol_corrupt`). A young part's CORRUPT must stay unmarked (test_cluster margins checks that through the
hook's `corrupt-young`). The flag is cleared only by a restart; R9's repair should clear it.

## Nothing declares a member destroyed before R9

`tsdb cluster node destroyed` (R9) replaces the hook's `destroyed` line. jepsen's `outage_entry_failed` treats LOST as
failed for good, but tsdbd puts no LOST into TOPOLOGY until protocol 16, so that branch is reached only by test_margins.

## TOPOLOGY

The margins and LOST health go into TOPOLOGY with protocol 16 (the change feed's), with test_wire cases then.
